# Estudio: Lambda `guardar-log-dynamodb` + DynamoDB + GitHub Actions

Este documento explica, de la forma más simple posible, todo lo necesario para entender y desplegar este repositorio:

1. Qué hace la función Lambda.
2. Cómo funciona el workflow de GitHub Actions.
3. Qué pasos hay que hacer en la consola web de AWS antes del primer `git push`.

---

## 1. Resumen del proyecto

La idea es esta:

```text
Consumer de Kafka  --->  Lambda guardar-log-dynamodb  --->  Tabla LogsCompras (DynamoDB)
```

- Un **Consumer de Kafka** envía un evento (un JSON) a la Lambda.
- La **Lambda** toma ese JSON, le agrega un `id` y una `fecha`, y lo guarda en **DynamoDB**.
- Cada vez que hacemos `git push` a la rama `main`, **GitHub Actions** empaqueta la Lambda y actualiza el código en AWS automáticamente.

Flujo de despliegue:

```text
git push origin main
        |
        v
GitHub Actions
  1. descarga el repositorio
  2. configura las credenciales de AWS
  3. crea function.zip con el contenido de /guardar-log-dynamodb
  4. ejecuta aws lambda update-function-code
        |
        v
Lambda actualizada en AWS (versión $LATEST)
```

---

## 2. Estructura del repositorio

```text
kafka-consumers-node/
├── .github/
│   └── workflows/
│       └── deploy-lambda.yml        <-- CI/CD con GitHub Actions
├── guardar-log-dynamodb/
│   └── index.js                     <-- código de la Lambda
├── .gitignore
└── ESTUDIO_LAMBDAS.md               <-- este documento (único .md)
```

No hay `package.json` ni `node_modules` porque el runtime **Node.js 20.x** de AWS Lambda ya trae incluido el AWS SDK v3 (`@aws-sdk/client-dynamodb` y `@aws-sdk/lib-dynamodb`). Por eso el ZIP solo necesita el archivo `index.js`.

---

## 3. Cómo funciona la Lambda, paso a paso

### 3.1. Entrada: el evento de Kafka

AWS ejecuta la función `handler` cada vez que llega un evento. El evento puede llegar de dos formas:

**Forma A: objeto JSON directo**

```json
{
  "tipo": "compra",
  "compraId": "C-1001",
  "monto": 25990
}
```

**Forma B: el JSON viene dentro de `event.body`** (por ejemplo, si hay un API Gateway en medio)

```json
{
  "body": "{\"tipo\":\"compra\",\"compraId\":\"C-1002\",\"monto\":15990}"
}
```

El código revisa si existe `event.body`. Si existe y es texto, hace `JSON.parse()`; si ya es un objeto, lo usa tal cual. Si no existe, usa el evento completo.

### 3.2. Los dos clientes de DynamoDB

```js
const clienteDynamo = new DynamoDBClient({});
const clienteDocumento = DynamoDBDocumentClient.from(clienteDynamo);
```

- `DynamoDBClient` es el cliente de bajo nivel: se conecta con el servicio DynamoDB.
- `DynamoDBDocumentClient` es un "traductor": permite guardar objetos JavaScript normales sin escribir los tipos de DynamoDB (`S`, `N`, `M`, etc.).

La región no se escribe en el código. AWS Lambda configura automáticamente la variable de entorno `AWS_REGION`, y el SDK la usa sola. Por eso la tabla y la función deben estar **en la misma región**.

### 3.3. Construcción del registro

```js
const registro = {
  id: Date.now().toString(),
  fecha: new Date().toISOString(),
  evento: datos,
};
```

- `id`: número de milisegundos actuales convertido a texto. Es simple y sirve para la actividad.
- `fecha`: fecha actual en formato ISO, por ejemplo `2026-11-24T18:30:00.000Z`.
- `evento`: el JSON completo recibido (se guarda como un mapa dentro de DynamoDB).

> Nota para la defensa: `Date.now()` podría repetirse si llegan dos eventos en el mismo milisegundo. Para producción se usaría `crypto.randomUUID()`.

### 3.4. Guardado con `PutCommand`

```js
await clienteDocumento.send(
  new PutCommand({
    TableName: "LogsCompras",
    Item: registro,
  })
);
```

`PutCommand` inserta el registro en la tabla `LogsCompras`. Se usa `await` porque la operación es asíncrona: hay que esperar la respuesta de AWS antes de continuar.

Si la tabla no existe, si la región está mal o si el rol no tiene permisos, este `await` lanza un error y lo captura el `catch`.

### 3.5. Logs y respuesta

Antes y después de guardar hay `console.log()`:

```js
console.log("Evento recibido:", ...);
console.log("Guardando en DynamoDB:", ...);
console.log("Log guardado correctamente. id:", registro.id);
```

Estos mensajes se ven en **CloudWatch Logs** y sirven para demostrar que la función se ejecutó y para depurar errores.

Si todo sale bien:

```js
return { statusCode: 200, body: "Log guardado" };
```

Si algo falla:

```js
return { statusCode: 500, body: "Error al guardar el log" };
```

> Importante: devolver un `statusCode: 500` **no hace fallar** la invocación de Lambda; solo devuelve ese valor. En un sistema real, si queremos que Kafka reintente el mensaje, habría que lanzar el error con `throw error` para que la invocación quede como fallida. Para esta actividad se mantiene el retorno simple.

---

## 4. Código completo y comentado de `index.js`

```js
// ============================================================
// Lambda: guardar-log-dynamodb
// Recibe un evento de un Consumer de Kafka y lo guarda en DynamoDB.
// Runtime: Node.js 20.x (CommonJS, por eso usamos require)
// ============================================================

// Cliente "base" de DynamoDB: se encarga de la conexión con AWS.
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");

// Cliente "documento": permite enviar objetos JavaScript normales
// sin tener que escribir los tipos de DynamoDB a mano.
const { DynamoDBDocumentClient, PutCommand } = require("@aws-sdk/lib-dynamodb");

// La región (por ejemplo us-east-1) la toma sola desde la variable
// de entorno AWS_REGION que AWS Lambda configura automáticamente.
const clienteDynamo = new DynamoDBClient({});
const clienteDocumento = DynamoDBDocumentClient.from(clienteDynamo);

// Nombre de la tabla que se crea en la consola de AWS.
const NOMBRE_TABLA = "LogsCompras";

// Esta es la función que ejecuta AWS cuando llega un evento.
exports.handler = async (event) => {
  try {
    // 1) Obtener el JSON del evento.
    // Puede llegar como objeto directo o como texto dentro de event.body.
    let datos = event;

    if (event && event.body) {
      datos = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
    }

    console.log("Evento recibido:", JSON.stringify(datos));

    // 2) Crear el registro con los tres campos de la tabla.
    const registro = {
      id: Date.now().toString(),       // identificador simple y único
      fecha: new Date().toISOString(), // fecha actual en formato ISO
      evento: datos,                   // JSON completo recibido
    };

    console.log("Guardando en DynamoDB:", JSON.stringify(registro));

    // 3) Guardar el registro en la tabla LogsCompras.
    await clienteDocumento.send(
      new PutCommand({
        TableName: NOMBRE_TABLA,
        Item: registro,
      })
    );

    console.log("Log guardado correctamente. id:", registro.id);

    // 4) Responder que todo salió bien.
    return {
      statusCode: 200,
      body: "Log guardado",
    };
  } catch (error) {
    // Si algo falla (JSON inválido, permisos, tabla inexistente, etc.)
    // el error queda en CloudWatch y devolvemos un 500.
    console.error("Error al guardar el log:", error);

    return {
      statusCode: 500,
      body: "Error al guardar el log",
    };
  }
};
```

El archivo real está en `guardar-log-dynamodb/index.js`.

---

## 5. Cómo funciona el workflow de GitHub Actions

Archivo: `.github/workflows/deploy-lambda.yml`

### 5.1. Disparador

```yaml
on:
  push:
    branches:
      - main
```

Solo se ejecuta cuando se hace `push` a la rama `main`. Si se hace push a otra rama, no despliega.

### 5.2. Paso 1: descargar el código

```yaml
- uses: actions/checkout@v4
```

GitHub Actions crea una máquina virtual Ubuntu limpia. Este paso clona el repositorio dentro de esa máquina para poder usar los archivos.

### 5.3. Paso 2: configurar credenciales de AWS

```yaml
- uses: aws-actions/configure-aws-credentials@v4
  with:
    aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
    aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    aws-session-token: ${{ secrets.AWS_SESSION_TOKEN }}
    aws-region: ${{ secrets.AWS_REGION }}
```

Este paso deja disponibles las credenciales para los siguientes comandos de AWS CLI. Los valores **no están escritos en el repositorio**: se leen de los *secrets* de GitHub.

- `AWS_ACCESS_KEY_ID`: identificador de la clave.
- `AWS_SECRET_ACCESS_KEY`: clave secreta.
- `AWS_SESSION_TOKEN`: token temporal. Es obligatorio en AWS Academy / Learner Lab y opcional en una cuenta normal.
- `AWS_REGION`: región donde están la tabla y la Lambda (por ejemplo `us-east-1`).

### 5.4. Paso 3: empaquetar la Lambda

```yaml
run: |
  cd guardar-log-dynamodb
  zip -r ../function.zip .
```

Entra a la carpeta de la Lambda y comprime **su contenido**. Es importante comprimir el contenido y no la carpeta, para que dentro del ZIP quede `index.js` en la raíz:

```text
function.zip
└── index.js        <-- correcto
```

Si quedara `guardar-log-dynamodb/index.js`, AWS no encontraría el handler.

No se ejecuta `npm install` porque el AWS SDK v3 ya viene en el runtime Node.js 20.

### 5.5. Paso 4: actualizar la función en AWS

```yaml
run: |
  aws lambda update-function-code \
    --function-name guardar-log-dynamodb \
    --zip-file fileb://function.zip
```

- `--function-name guardar-log-dynamodb`: debe coincidir **exactamente** con el nombre de la función creada en la consola.
- `--zip-file fileb://function.zip`: sube el archivo binario `function.zip`.
- Este comando actualiza el código de la versión `$LATEST`. No crea una versión nueva ni cambia configuración (runtime, handler, memoria, etc.).

### 5.6. Resumen visual del workflow

```text
push a main
   |
   v
[checkout] --> [configure-aws-credentials] --> [zip] --> [update-function-code]
                                                              |
                                                              v
                                                   Lambda $LATEST actualizada
```

---

## 6. Preparación en la consola web de AWS (antes del primer push)

> Haz estos pasos en la **misma región** que usarás en el secret `AWS_REGION` (por ejemplo `us-east-1`). Si la tabla está en una región y la Lambda en otra, el código fallará.

### 6.1. Crear la tabla `LogsCompras` en DynamoDB

1. Entra a la consola de AWS y busca **DynamoDB**.
2. En el menú izquierdo, haz clic en **Tables** y luego en **Create table**.
3. Completa:
   - **Table name**: `LogsCompras`
   - **Partition key**: `id`
   - **Partition key type**: `String`
   - **Sort key**: déjalo vacío (no se necesita).
4. En **Table settings**, deja la opción **Default settings** (capacidad *On-demand*). Es la más simple y no hay que administrar capacidad.
5. Haz clic en **Create table** y espera hasta que el estado sea **Active**.
6. Abre la tabla y copia su **ARN**. Lo encontrarás en **Additional info** o en la pestaña **Overview**. Tendrá esta forma:

```text
arn:aws:dynamodb:us-east-1:123456789012:table/LogsCompras
```

Ese ARN se usa en el siguiente paso. Reemplaza `123456789012` por el ID de tu cuenta (lo ves en la esquina superior derecha de la consola).

### 6.2. Crear el rol de IAM y darle permisos de DynamoDB

La Lambda necesita dos permisos:

1. Escribir logs en CloudWatch (para que se vean los `console.log`).
2. Hacer `PutItem` en la tabla `LogsCompras`.

#### Opción A: crear un rol nuevo (cuenta normal)

1. Busca **IAM** en la consola.
2. Ve a **Roles** → **Create role**.
3. En **Trusted entity type**, elige **AWS service**.
4. En **Use case**, elige **Lambda** y haz clic en **Next**.
5. En la lista de permisos, busca y marca **AWSLambdaBasicExecutionRole**. Esto permite escribir en CloudWatch Logs.
6. Haz clic en **Next**.
7. En **Role name**, escribe: `rol-guardar-log-dynamodb`.
8. Haz clic en **Create role**.
9. Abre el rol recién creado y haz clic en **Add permissions** → **Create inline policy**.
10. Cambia a la pestaña **JSON** y pega esta política (reemplaza el ARN por el tuyo):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:PutItem",
      "Resource": "arn:aws:dynamodb:us-east-1:123456789012:table/LogsCompras"
    }
  ]
}
```

11. Haz clic en **Next**.
12. En **Policy name**, escribe: `PermitirPutItemLogsCompras`.
13. Haz clic en **Create policy**.

Con esto el rol puede escribir en la tabla y en CloudWatch.

#### Opción B: AWS Academy / Learner Lab

En algunos laboratorios no se permite crear roles nuevos. En ese caso:

1. Al crear la Lambda, elige el rol que ya existe en el laboratorio (normalmente se llama **LabRole**).
2. Verifica que ese rol tenga permisos sobre DynamoDB. Si no los tiene y el laboratorio te permite editar políticas, agrégale la política inline del punto anterior.
3. Si el laboratorio no permite editar IAM, consulta con el docente: puede que el rol del laboratorio ya incluya permisos amplios o que haya que usar otro rol asignado.

> El rol debe existir **antes** de crear la función Lambda.

### 6.3. Crear la función Lambda vacía

1. Busca **Lambda** en la consola y haz clic en **Create function**.
2. Elige **Author from scratch**.
3. Completa:
   - **Function name**: `guardar-log-dynamodb` (exactamente así, porque el workflow usa ese nombre).
   - **Runtime**: `Node.js 20.x`.
   - **Architecture**: `x86_64`.

   > Si la consola ya no muestra `Node.js 20.x` porque AWS lo marcó como obsoleto, elige `Node.js 22.x`. El código es compatible con ambos runtimes porque usa CommonJS y el AWS SDK v3.
4. En **Permissions**, elige **Use an existing role**.
5. Selecciona el rol creado en el paso 6.2 (`rol-guardar-log-dynamodb` o el rol del laboratorio).
6. Haz clic en **Create function**.
7. Ya dentro de la función, ve a **Configuration** → **General configuration** → **Edit**:
   - **Handler**: `index.handler`
   - **Timeout**: puedes subirlo a `10` segundos para tener margen (por defecto son 3).
   - **Memory**: `128 MB` es suficiente.
8. Guarda los cambios.

No hace falta subir el código a mano: el primer `git push` lo hará por ti. La función solo debe existir para que `update-function-code` tenga dónde escribir.

> La variable `AWS_REGION` no se configura a mano. Lambda la define automáticamente. El SDK la lee sola.

---

## 7. Configurar GitHub (secrets)

1. Sube el repositorio a GitHub (o usa el que ya tienes creado).
2. Entra al repositorio en GitHub.
3. Ve a **Settings** → **Secrets and variables** → **Actions**.
4. Haz clic en **New repository secret** y crea estos secrets:

| Nombre | Valor |
|---|---|
| `AWS_ACCESS_KEY_ID` | Tu access key ID |
| `AWS_SECRET_ACCESS_KEY` | Tu secret access key |
| `AWS_SESSION_TOKEN` | Tu session token (obligatorio en AWS Academy; si no usas, puedes dejarlo vacío o no crearlo) |
| `AWS_REGION` | Región de la tabla y la Lambda, por ejemplo `us-east-1` |

### Cómo obtener las credenciales en AWS Academy

1. Entra al **Learner Lab**.
2. Haz clic en **AWS Details**.
3. Junto a **AWS CLI**, haz clic en **Show**.
4. Copia los tres valores:

```text
aws_access_key_id=...
aws_secret_access_key=...
aws_session_token=...
```

5. Pégalos en los secrets correspondientes de GitHub.

> Las credenciales de AWS Academy son temporales: caducan cuando termina la sesión del laboratorio. Si el workflow falla con `ExpiredToken`, vuelve a copiar las credenciales y actualiza los secrets.

**Nunca** escribas estas claves en el código ni las subas al repositorio.

---

## 8. Primer `git push` y primer despliegue

Si el repositorio ya está clonado y tiene el remoto configurado:

```bash
cd kafka-consumers-node
git add .
git commit -m "Agrega Lambda guardar-log-dynamodb y CI/CD"
git push origin main
```

Si estuvieras creando el repositorio desde cero:

```bash
git init
git branch -M main
git remote add origin https://github.com/TU_USUARIO/TU_REPOSITORIO.git
git add .
git commit -m "Agrega Lambda guardar-log-dynamodb y CI/CD"
git push -u origin main
```

Al hacer push:

1. GitHub detecta el archivo `.github/workflows/deploy-lambda.yml`.
2. Se abre la pestaña **Actions** y comienza el workflow **Deploy Lambda guardar-log-dynamodb**.
3. Si todo está bien, aparece un check verde.
4. Si algo falla, haz clic en el workflow y luego en el paso rojo para leer el error.

---

## 9. Verificar que el despliegue funcionó

1. Ve a **GitHub** → pestaña **Actions**.
2. Abre el workflow y confirma que los 4 pasos terminaron en verde.
3. Ve a la consola de **AWS Lambda** → `guardar-log-dynamodb`.
4. En **Code**, deberías ver el `index.js` desplegado (puedes refrescar la página).
5. Revisa **Last modified** para confirmar la fecha de actualización.

---

## 10. Probar la Lambda desde la consola

1. Abre la función `guardar-log-dynamodb`.
2. Ve a la pestaña **Test**.
3. Elige **Create new event**.
4. Escribe un nombre, por ejemplo `evento-compra`.
5. Pega uno de estos eventos:

**Evento directo:**

```json
{
  "tipo": "compra",
  "compraId": "C-1001",
  "monto": 25990,
  "cliente": "cristopher"
}
```

**Evento con `body` (texto JSON):**

```json
{
  "body": "{\"tipo\":\"compra\",\"compraId\":\"C-1002\",\"monto\":15990,\"cliente\":\"cristopher\"}"
}
```

6. Haz clic en **Test**.
7. Deberías ver una respuesta como:

```json
{
  "statusCode": 200,
  "body": "Log guardado"
}
```

8. Ve a **DynamoDB** → **Tables** → `LogsCompras` → **Explore table items**.
9. Debería aparecer un registro nuevo con esta forma:

```json
{
  "id": "1732490000000",
  "fecha": "2026-11-24T18:30:00.000Z",
  "evento": {
    "tipo": "compra",
    "compraId": "C-1001",
    "monto": 25990,
    "cliente": "cristopher"
  }
}
```

10. Para ver los `console.log`, ve a **Monitor** → **View CloudWatch logs** → abre el log stream más reciente.

---

## 11. Errores comunes y cómo resolverlos

| Error | Causa probable | Solución |
|---|---|---|
| `ExpiredToken` / `InvalidClientTokenId` | Las credenciales de AWS Academy caducaron | Copiar credenciales nuevas del Learner Lab y actualizar los secrets |
| `ResourceNotFoundException` | La tabla no existe o está en otra región | Verificar el nombre `LogsCompras` y que la tabla esté en la región del secret `AWS_REGION` |
| `AccessDeniedException` al hacer `PutItem` | El rol no tiene la política de DynamoDB o el ARN está mal | Revisar la política inline y el ARN de la tabla |
| `Function not found` | La función no existe o el nombre/región no coincide | Crear `guardar-log-dynamodb` en la misma región |
| `Cannot find module '@aws-sdk/client-dynamodb'` | El runtime no es Node.js 18/20 | Cambiar el runtime a **Node.js 20.x** |
| `Runtime.HandlerNotFound` | El handler no se llama `index.handler` o el ZIP tiene la carpeta dentro | Revisar el handler y empaquetar el contenido de la carpeta, no la carpeta |
| El workflow falla en el `zip` | La carpeta `guardar-log-dynamodb` no existe en la raíz | Verificar la estructura del repositorio |
| `Task timed out` | La Lambda no alcanzó a responder en 3 segundos | Subir el timeout a 10 segundos en **General configuration** |
| `SyntaxError: Unexpected token` | `event.body` no contiene JSON válido | Enviar un JSON válido dentro de `body` |

---

## 12. Puntos clave para la defensa oral

Estas son las ideas que conviene poder explicar:

1. **La Lambda recibe un evento de Kafka** y guarda el JSON en DynamoDB. No usa frameworks: solo Node.js 20 y el AWS SDK v3.
2. **`DynamoDBDocumentClient`** simplifica el código porque permite guardar objetos JavaScript sin escribir los tipos de DynamoDB.
3. **La región se toma de `AWS_REGION`**, variable que Lambda configura automáticamente. Por eso no está escrita en el código.
4. **El `id` se genera con `Date.now()`** y la `fecha` con `new Date().toISOString()`. Se explicó que para producción sería mejor un UUID.
5. **El `try/catch`** evita que un error tumbe la función sin control; el error se registra en CloudWatch y se devuelve un `500` simple.
6. **GitHub Actions automatiza el despliegue**: en cada push a `main` descarga el código, configura las credenciales, crea `function.zip` y ejecuta `aws lambda update-function-code`.
7. **Los secrets** mantienen las credenciales fuera del repositorio. En AWS Academy se agrega `AWS_SESSION_TOKEN` porque las credenciales son temporales.
8. **No se sube `node_modules`** porque el runtime Node.js 20 ya incluye el SDK de AWS; el ZIP solo contiene `index.js`.
9. **El workflow actualiza `$LATEST`**. No crea versiones ni aliases; eso sería un siguiente paso si se quisiera un despliegue más controlado.
10. **Mejora de seguridad para producción**: en lugar de claves de larga duración, usar OIDC entre GitHub Actions y AWS. En el laboratorio se usan claves porque AWS Academy no siempre permite OIDC.
