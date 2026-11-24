# Estudio completo: Consumers de Kafka + Lambda + DynamoDB + GitHub Actions

Este documento explica, de la forma más simple posible, todo lo necesario para entender y desplegar este repositorio. Está dividido en dos partes:

- **Parte I (secciones 1 a 12):** la función Lambda `guardar-log-dynamodb`, su tabla en DynamoDB y su workflow de despliegue.
- **Parte II (secciones 13 a 19):** los 3 consumers de Kafka del topic `compras-topic`, su despliegue en una EC2 con PM2 y el diagrama C3.

Todo el proyecto usa JavaScript puro y CommonJS, sin frameworks.

---

## 1. Resumen del proyecto

La idea es esta:

```text
                         +--> grupo-logs-dynamo ------> Lambda guardar-log-dynamodb --> DynamoDB
Topic compras-topic -----+--> grupo-pago-validacion --> valida monto > 0
                         +--> grupo-pago-comprobante --> genera código de boleta
```

- Un **producer** escribe mensajes JSON en el topic `compras-topic`.
- Los **3 consumers** leen el mismo topic, pero cada uno con un `groupId` distinto. Kafka entrega una copia de cada mensaje a cada grupo.
- El consumer de logs invoca la **Lambda**, y la Lambda guarda el evento en **DynamoDB**.
- Los otros dos consumers hacen la validación del pago y la generación de la boleta.
- Hay **dos workflows** de GitHub Actions:
  - `deploy-lambda.yml`: se ejecuta en cada push a `main` y actualiza la Lambda. Así se definió en la Parte I y no se modificó.
  - `deploy-consumers.yml`: se ejecuta solo cuando cambia `consumers/**` y despliega los consumers por SSH en la EC2.

> El workflow de la Lambda no tiene filtro de rutas, así que también se ejecutará cuando hagas un push que solo cambie `consumers/**`. Es el comportamiento actual del CI/CD existente; si se quisiera separar, habría que agregarle un `paths` a `deploy-lambda.yml`.

Flujo de despliegue de la Lambda:

```text
git push origin main
        |
        v
GitHub Actions (deploy-lambda.yml)
  1. descarga el repositorio
  2. configura las credenciales de AWS
  3. crea function.zip con el contenido de /guardar-log-dynamodb
  4. ejecuta aws lambda update-function-code
        |
        v
Lambda actualizada en AWS (versión $LATEST)
```

Flujo de despliegue de los consumers:

```text
git push origin main (con cambios en consumers/**)
        |
        v
GitHub Actions (deploy-consumers.yml)
  1. se conecta por SSH a la EC2
  2. entra a /home/ec2-user/kafka-consumers-node/consumers
  3. ejecuta git pull, npm install y pm2 restart (o pm2 start)
        |
        v
Consumers actualizados en la EC2
```

---

## 2. Estructura del repositorio

```text
kafka-consumers-node/
├── .github/
│   └── workflows/
│       ├── deploy-lambda.yml        <-- CI/CD de la Lambda
│       └── deploy-consumers.yml     <-- CI/CD de los consumers
├── guardar-log-dynamodb/
│   └── index.js                     <-- código de la Lambda
├── consumers/
│   ├── package.json                 <-- dependencias de los consumers
│   ├── .env.example                 <-- variables de entorno de ejemplo
│   └── index.js                     <-- los 3 consumers de Kafka
├── .gitignore
└── ESTUDIO_LAMBDAS.md               <-- este documento (único .md)
```

La carpeta `guardar-log-dynamodb` no tiene `package.json` ni `node_modules` porque el runtime **Node.js 20.x** de AWS Lambda ya trae incluido el AWS SDK v3 (`@aws-sdk/client-dynamodb` y `@aws-sdk/lib-dynamodb`). Por eso el ZIP solo necesita el archivo `index.js`.

La carpeta `consumers` sí tiene su propio `package.json`, porque se ejecuta en una EC2 con Node.js y necesita instalar `kafkajs`, `@aws-sdk/client-lambda` y `dotenv`.

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

## 5. Cómo funciona el workflow de la Lambda (`deploy-lambda.yml`)

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
git commit -m "Agrega Lambda, consumers de Kafka y CI/CD"
git push origin main
```

Si estuvieras creando el repositorio desde cero:

```bash
git init
git branch -M main
git remote add origin https://github.com/TU_USUARIO/TU_REPOSITORIO.git
git add .
git commit -m "Agrega Lambda, consumers de Kafka y CI/CD"
git push -u origin main
```

> Antes de este push, si ya agregaste la carpeta `consumers`, prepara la EC2 y los secrets `EC2_HOST`, `EC2_USER` y `EC2_SSH_KEY` (sección 13.9). Si no están configurados, el workflow `deploy-consumers` fallará.

Al hacer push:

1. GitHub detecta los dos workflows: `deploy-lambda.yml` y `deploy-consumers.yml`.
2. Se abre la pestaña **Actions**. El workflow **Deploy Lambda guardar-log-dynamodb** corre en cada push a `main`.
3. El workflow **Deploy consumers Kafka** solo corre si el push incluye cambios en `consumers/**`.
4. Si todo está bien, aparecen los checks verdes.
5. Si algo falla, haz clic en el workflow y luego en el paso rojo para leer el error.

---

## 9. Verificar que el despliegue de la Lambda funcionó

1. Ve a **GitHub** → pestaña **Actions**.
2. Abre el workflow y confirma que los 4 pasos terminaron en verde.
3. Ve a la consola de **AWS Lambda** → `guardar-log-dynamodb`.
4. En **Code**, deberías ver el `index.js` desplegado (puedes refrescar la página).
5. Revisa **Last modified** para confirmar la fecha de actualización.

> La verificación del despliegue de los consumers está en la sección 13.10.

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

---

# PARTE II: Consumers de Kafka

## 13. Consumers de Kafka

### 13.1. Qué hace cada consumer

Los tres consumers leen el mismo topic, `compras-topic`, pero pertenecen a grupos distintos. Eso hace que Kafka le entregue **una copia del mismo mensaje a cada grupo**.

| Consumer | `groupId` | Lógica | Salida esperada |
|---|---|---|---|
| 1. Logs | `grupo-logs-dynamo` | Invoca la Lambda `guardar-log-dynamodb` con el JSON del mensaje | `[LOGS DYNAMO] Lambda invocada...` |
| 2. Validación | `grupo-pago-validacion` | Convierte `monto` a número y verifica que sea mayor que 0 | `[VALIDACION PAGO] Pago aprobado` |
| 3. Comprobante | `grupo-pago-comprobante` | Genera un código aleatorio de boleta | `[COMPROBANTE PAGO] Boleta generada` |

Los tres se levantan desde un solo archivo `consumers/index.js` y un solo proceso de Node.js. PM2 se encarga de mantener ese proceso vivo en la EC2 con el nombre `consumers`.

### 13.2. Por qué los `groupId` deben ser distintos

Esta es la parte más importante de Kafka para la defensa oral:

```text
Mismo groupId:
  mensaje 1 --> consumer A
  mensaje 2 --> consumer B
  (Kafka reparte los mensajes entre los consumers del grupo)

Distinto groupId:
  mensaje 1 --> grupo 1 (recibe una copia)
  mensaje 1 --> grupo 2 (recibe otra copia)
  mensaje 1 --> grupo 3 (recibe otra copia)
  (cada grupo recibe todos los mensajes)
```

Por eso:

- Si los tres consumers usaran `groupId: "mi-grupo"`, Kafka repartiría los mensajes entre ellos y **no todos** ejecutarían la misma lógica.
- Al usar `grupo-logs-dynamo`, `grupo-pago-validacion` y `grupo-pago-comprobante`, cada grupo recibe el 100% de los mensajes y puede hacer su propia tarea.

En el código se usa `fromBeginning: true`. Eso significa que, la primera vez que un grupo se conecta, lee los mensajes que ya estaban en el topic. Después Kafka guarda el offset del grupo y continúa desde donde quedó. En producción normalmente se usa `fromBeginning: false` para no reprocesar mensajes antiguos.

### 13.3. Estructura y variables de entorno

```text
consumers/
├── package.json      <-- dependencias y script npm start
├── .env.example      <-- ejemplo de variables (sí se sube al repo)
├── .env              <-- variables reales (NO se sube, está en .gitignore)
└── index.js          <-- los 3 consumers
```

Variables:

| Variable | Para qué sirve | Ejemplo |
|---|---|---|
| `KAFKA_BROKERS` | Dirección(es) del broker. Si hay varias, van separadas por coma | `localhost:9092` |
| `KAFKA_TOPIC` | Topic que se consume | `compras-topic` |
| `LAMBDA_LOGS_NAME` | Nombre exacto de la Lambda | `guardar-log-dynamodb` |
| `AWS_REGION` | Región de la Lambda | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | Credencial de AWS | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | Clave secreta de AWS | `...` |
| `AWS_SESSION_TOKEN` | Token temporal de AWS Academy | `IQoJ...` |

`dotenv` lee el archivo `.env` al iniciar el proceso. El archivo `.env.example` se sube al repositorio como documentación, pero el `.env` real está en `.gitignore` y **nunca** debe subirse.

### 13.4. Diagrama C3 en Mermaid

El modelo C4 tiene cuatro niveles: contexto (C1), contenedores (C2), componentes (C3) y código (C4). Este es el **diagrama C3**, es decir, el nivel de componentes: muestra los 3 consumers, sus grupos y las piezas con las que se comunican.

```mermaid
C4Component
title Diagrama C3 - Componentes de los consumers de compras

Container(kafka, "Apache Kafka", "Mensajería", "Topic compras-topic")
Container_Boundary(ec2, "EC2 Amazon Linux") {
  Component(cLogs, "Consumer 1 - Logs", "Node.js + KafkaJS", "groupId: grupo-logs-dynamo")
  Component(cPago, "Consumer 2 - Validación", "Node.js + KafkaJS", "groupId: grupo-pago-validacion")
  Component(cBoleta, "Consumer 3 - Comprobante", "Node.js + KafkaJS", "groupId: grupo-pago-comprobante")
}
Container(lambda, "Lambda guardar-log-dynamodb", "Node.js 20", "Guarda el evento en DynamoDB")
ContainerDb(dynamo, "DynamoDB", "NoSQL", "Tabla LogsCompras")
Container_Ext(github, "GitHub Actions", "CI/CD", "Despliegue por SSH")

Rel(cLogs, kafka, "Consume", "Kafka")
Rel(cPago, kafka, "Consume", "Kafka")
Rel(cBoleta, kafka, "Consume", "Kafka")
Rel(cLogs, lambda, "Invoca con el JSON", "AWS SDK")
Rel(lambda, dynamo, "PutItem", "AWS SDK")
Rel(github, ec2, "Despliega los 3 consumers", "SSH")
```

Cómo leerlo:

- **Apache Kafka** es el origen de los mensajes.
- Los **3 consumers** corren dentro de la **EC2 Amazon Linux**, en un proceso de PM2 llamado `consumers`.
- Solo el consumer de logs invoca la **Lambda**.
- La **Lambda** escribe en **DynamoDB**.
- **GitHub Actions** se conecta por SSH a la EC2 para actualizar el código.

### 13.5. Cómo funciona `consumers/index.js`, paso a paso

1. **Carga las variables de entorno**

   ```js
   require("dotenv").config();
   ```

   Esto lee `consumers/.env` y deja los valores disponibles en `process.env`.

2. **Crea el cliente de Lambda**

   ```js
   const clienteLambda = new LambdaClient({
     region: process.env.AWS_REGION || "us-east-1",
   });
   ```

   El SDK busca las credenciales en `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` y `AWS_SESSION_TOKEN`. No están escritas en el código.

3. **Crea un solo cliente de Kafka**

   ```js
   const kafka = new Kafka({
     clientId: "consumidores-compras",
     brokers: BROKERS,
   });
   ```

   `BROKERS` se arma separando `KAFKA_BROKERS` por comas, por si hay más de un broker.

4. **Define los tres consumers**

   ```js
   const definiciones = [
     { nombre: "logs-dynamodb", groupId: "grupo-logs-dynamo", procesar: procesarLogDynamo },
     { nombre: "validacion-pago", groupId: "grupo-pago-validacion", procesar: procesarValidacionPago },
     { nombre: "comprobante-pago", groupId: "grupo-pago-comprobante", procesar: procesarComprobantePago },
   ];
   ```

   Cada definición tiene su `groupId` y la función que procesa el mensaje.

5. **El consumer de logs invoca la Lambda**

   ```js
   const respuesta = await clienteLambda.send(
     new InvokeCommand({
       FunctionName: LAMBDA_LOGS_NAME,
       InvocationType: "RequestResponse",
       Payload: JSON.stringify(evento),
     })
   );
   ```

   - `FunctionName` viene de `LAMBDA_LOGS_NAME`.
   - `Payload` es el JSON del mensaje, convertido a texto.
   - `RequestResponse` espera la respuesta de la Lambda (no es asíncrono).

6. **El consumer de validación revisa el monto**

   ```js
   const monto = Number(evento.monto);

   if (monto > 0) {
     console.log("[VALIDACION PAGO] Pago aprobado");
     console.log(`[VALIDACION PAGO] Monto validado: ${monto}`);
   } else {
     console.log("[VALIDACION PAGO] Pago rechazado");
     console.log(`[VALIDACION PAGO] Monto inválido: ${evento.monto}`);
   }
   ```

   Si `monto` no existe o no es un número, `Number(...)` devuelve `NaN`, y `NaN > 0` es falso, así que el pago se rechaza.

7. **El consumer de comprobante genera un código**

   ```js
   const codigoBoleta = Math.random().toString(36).slice(2, 8).toUpperCase();
   console.log("[COMPROBANTE PAGO] Boleta generada");
   console.log(`[COMPROBANTE PAGO] Código de boleta: ${codigoBoleta}`);
   ```

   `Math.random()` genera un número, se pasa a base 36 y se toman 6 caracteres. Es simple y sirve para la actividad; en producción se usaría un UUID.

8. **Convierte el mensaje de Kafka a JSON**

   ```js
   const texto = message.value.toString();

   try {
     return JSON.parse(texto);
   } catch (error) {
     console.error("[CONSUMER] Mensaje ignorado, no es JSON válido:", texto);
     return null;
   }
   ```

   Kafka entrega `message.value` como un `Buffer`, por eso primero se convierte a texto. Si el mensaje no es JSON válido, se ignora y se registra el error.

9. **Conecta cada consumer y se suscribe al topic**

   ```js
   await consumer.connect();
   await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
   await consumer.run({ eachMessage: async (...) => { ... } });
   ```

   `consumer.run()` deja al consumer escuchando. Por cada mensaje se ejecuta `eachMessage`.

10. **Cierra ordenadamente con Ctrl+C**

    ```js
    process.on("SIGINT", cerrar);
    process.on("SIGTERM", cerrar);
    ```

    Al recibir `Ctrl+C` o una señal de PM2, se desconectan los consumers antes de salir.

> Nota: si `procesar` lanza un error, el `try/catch` de `eachMessage` lo registra y el consumer sigue vivo. En un sistema real habría que decidir si se relanza el error para que Kafka reintente o si se envía a una cola de mensajes fallidos (DLQ).

### 13.6. Código completo y comentado de `consumers/index.js`

```js
// ============================================================
// Consumers de Kafka - compras-topic
// 3 grupos distintos; cada grupo recibe una copia de cada mensaje.
// Runtime: Node.js 20 (CommonJS)
// ============================================================

require("dotenv").config();

const { Kafka } = require("kafkajs");
const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");

// ---------- Configuración ----------
const BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((broker) => broker.trim());

const TOPIC = process.env.KAFKA_TOPIC || "compras-topic";
const LAMBDA_LOGS_NAME = process.env.LAMBDA_LOGS_NAME || "guardar-log-dynamodb";

// El cliente de Lambda lee las credenciales desde las variables de entorno
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY y AWS_SESSION_TOKEN.
const clienteLambda = new LambdaClient({
  region: process.env.AWS_REGION || "us-east-1",
});

// Un solo cliente de Kafka. A partir de él se crean los 3 consumers.
const kafka = new Kafka({
  clientId: "consumidores-compras",
  brokers: BROKERS,
});

// Guardamos las instancias para poder desconectarlas con Ctrl+C.
const consumidores = [];
let cerrando = false;

// ---------- Lógica de cada consumer ----------
async function procesarLogDynamo(evento) {
  const respuesta = await clienteLambda.send(
    new InvokeCommand({
      FunctionName: LAMBDA_LOGS_NAME,
      InvocationType: "RequestResponse",
      Payload: JSON.stringify(evento),
    })
  );

  const textoRespuesta = respuesta.Payload
    ? Buffer.from(respuesta.Payload).toString()
    : "(sin respuesta)";

  console.log(`[LOGS DYNAMO] Lambda invocada (status ${respuesta.StatusCode})`);
  console.log(`[LOGS DYNAMO] Respuesta: ${textoRespuesta}`);

  if (respuesta.FunctionError) {
    console.error(`[LOGS DYNAMO] La Lambda falló: ${respuesta.FunctionError}`);
  }
}

async function procesarValidacionPago(evento) {
  const monto = Number(evento.monto);

  if (monto > 0) {
    console.log("[VALIDACION PAGO] Pago aprobado");
    console.log(`[VALIDACION PAGO] Monto validado: ${monto}`);
  } else {
    console.log("[VALIDACION PAGO] Pago rechazado");
    console.log(`[VALIDACION PAGO] Monto inválido: ${evento.monto}`);
  }
}

async function procesarComprobantePago(evento) {
  // Código simple de 6 caracteres para la actividad.
  const codigoBoleta = Math.random().toString(36).slice(2, 8).toUpperCase();

  console.log("[COMPROBANTE PAGO] Boleta generada");
  console.log(`[COMPROBANTE PAGO] Código de boleta: ${codigoBoleta}`);
}

// Los tres consumers leen el mismo topic, pero cada uno tiene su groupId.
const definiciones = [
  {
    nombre: "logs-dynamodb",
    groupId: "grupo-logs-dynamo",
    procesar: procesarLogDynamo,
  },
  {
    nombre: "validacion-pago",
    groupId: "grupo-pago-validacion",
    procesar: procesarValidacionPago,
  },
  {
    nombre: "comprobante-pago",
    groupId: "grupo-pago-comprobante",
    procesar: procesarComprobantePago,
  },
];

// ---------- Utilidades ----------
function leerMensaje(message) {
  if (!message.value) {
    return null;
  }

  const texto = message.value.toString();

  try {
    return JSON.parse(texto);
  } catch (error) {
    console.error("[CONSUMER] Mensaje ignorado, no es JSON válido:", texto);
    return null;
  }
}

// ---------- Inicio y cierre de los consumers ----------
async function iniciarConsumer(definicion) {
  const consumer = kafka.consumer({ groupId: definicion.groupId });
  consumidores.push(consumer);

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  console.log(`[${definicion.nombre}] Conectado. groupId=${definicion.groupId}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const evento = leerMensaje(message);

      if (evento === null) {
        return;
      }

      console.log(
        `[${definicion.nombre}] Mensaje recibido en ${topic} (partition=${partition}, offset=${message.offset})`
      );

      try {
        await definicion.procesar(evento);
      } catch (error) {
        console.error(`[${definicion.nombre}] Error al procesar el mensaje:`, error.message);
      }
    },
  });
}

async function cerrar() {
  if (cerrando) {
    return;
  }

  cerrando = true;
  console.log("Cerrando consumers...");

  await Promise.all(consumidores.map((consumer) => consumer.disconnect()));
  process.exit(0);
}

process.on("SIGINT", cerrar);
process.on("SIGTERM", cerrar);

async function main() {
  console.log(`Conectando a Kafka: ${BROKERS.join(", ")}`);
  console.log(`Topic: ${TOPIC}`);
  console.log(`Lambda: ${LAMBDA_LOGS_NAME}`);

  await Promise.all(definiciones.map((definicion) => iniciarConsumer(definicion)));
}

main().catch((error) => {
  console.error("Error fatal al iniciar los consumers:", error);
  process.exit(1);
});
```

El archivo real está en `consumers/index.js`.

### 13.7. `package.json` y `.env.example`

**`consumers/package.json`:**

```json
{
  "name": "consumers",
  "version": "1.0.0",
  "description": "Tres consumers de Kafka para el topic compras-topic",
  "main": "index.js",
  "type": "commonjs",
  "private": true,
  "scripts": {
    "start": "node index.js"
  },
  "engines": {
    "node": ">=20"
  },
  "dependencies": {
    "@aws-sdk/client-lambda": "^3.700.0",
    "dotenv": "^16.4.7",
    "kafkajs": "^2.2.4"
  }
}
```

- `kafkajs`: cliente de Kafka para Node.js.
- `@aws-sdk/client-lambda`: permite invocar la Lambda.
- `dotenv`: carga el archivo `.env`.

**`consumers/.env.example`:**

```dotenv
# Dirección(es) del broker de Kafka. Si hay varios, sepáralos con coma.
KAFKA_BROKERS=localhost:9092

# Topic que consumen los tres grupos.
KAFKA_TOPIC=compras-topic

# Nombre exacto de la Lambda que guarda los logs.
LAMBDA_LOGS_NAME=guardar-log-dynamodb

# Región de AWS donde está la Lambda.
AWS_REGION=us-east-1

# Credenciales de AWS.
# En AWS Academy también se usa AWS_SESSION_TOKEN.
# Si la EC2 usa un rol de instancia, puedes borrar estas tres líneas.
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
```

Para probar localmente:

```bash
cd consumers
npm install
cp .env.example .env
# editar .env con los valores reales
npm start
```

### 13.8. Cómo funciona `deploy-consumers.yml`

Archivo: `.github/workflows/deploy-consumers.yml`

```yaml
name: Deploy consumers Kafka

# Solo se ejecuta si cambia algo dentro de consumers/ en la rama main.
on:
  push:
    branches:
      - main
    paths:
      - "consumers/**"

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - name: Desplegar en la EC2 por SSH
        uses: appleboy/ssh-action@v1.2.5
        with:
          host: ${{ secrets.EC2_HOST }}
          username: ${{ secrets.EC2_USER }}
          key: ${{ secrets.EC2_SSH_KEY }}
          script_stop: true
          script: |
            cd /home/ec2-user/kafka-consumers-node/consumers
            git pull origin main
            npm install
            pm2 restart consumers || pm2 start index.js --name consumers
```

Paso a paso:

1. **`on.push.paths`**: este workflow solo corre si el push a `main` incluye cambios dentro de `consumers/`. Si cambias solo la Lambda, este workflow no se ejecuta.
   > Ojo: el workflow de la Lambda (`deploy-lambda.yml`) no tiene filtro de rutas y corre en cada push a `main`, así que también se ejecutará cuando cambies `consumers/**`. No se modificó porque la consigna indica no tocar el CI/CD existente.
2. **`appleboy/ssh-action`**: abre una conexión SSH a la EC2 usando la IP/DNS y la clave privada guardadas en los secrets.
3. **`git pull origin main`**: actualiza el código en la EC2. Por eso la EC2 debe tener el repositorio clonado y acceso de lectura al repositorio.
4. **`npm install`**: instala o actualiza las dependencias de `consumers/package.json` en la EC2.
5. **`pm2 restart consumers || pm2 start index.js --name consumers`**: si el proceso `consumers` ya existe, lo reinicia; si no existe, lo crea. El `||` permite usar el mismo comando para el primer despliegue y para los siguientes.
6. **`script_stop: true`**: si un comando falla, el workflow se detiene y no sigue con los demás.

> No se hace `actions/checkout` porque el código no se copia desde GitHub a la EC2: la EC2 lo descarga directamente con `git pull`. Tampoco se copia el `.env`: ese archivo debe existir en la EC2 y no se versiona.

### 13.9. Preparación de la EC2, IAM y secrets (antes del primer push)

#### A. Preparar la EC2 Amazon Linux

La EC2 debe tener Node.js, Git y PM2. Una secuencia posible en Amazon Linux 2023 es:

```bash
sudo dnf install -y git
curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
sudo dnf install -y nodejs
sudo npm install -g pm2
node --version
pm2 --version
```

Clonar el repositorio y preparar la carpeta:

```bash
cd /home/ec2-user
git clone https://github.com/TU_USUARIO/kafka-consumers-node.git
cd kafka-consumers-node/consumers
npm install
cp .env.example .env
nano .env
chmod 600 .env
```

En `.env` debes poner:

- La IP o DNS real del broker en `KAFKA_BROKERS`.
- Las credenciales de AWS.
- `AWS_SESSION_TOKEN` si usas AWS Academy.

Probar una primera ejecución manual:

```bash
node index.js
```

Si los tres consumers se conectan, detén con `Ctrl+C` y arráncalos con PM2:

```bash
pm2 start index.js --name consumers
pm2 save
pm2 status
```

Opcional: configurar PM2 para que se reinicie al reiniciar la EC2:

```bash
pm2 startup
# PM2 muestra un comando con sudo; cópialo y ejecútalo
pm2 save
```

#### B. Crear la clave SSH y los secrets de la EC2

1. En EC2, crea o usa un **key pair** de Amazon Linux. Descarga el archivo `.pem`.
2. En tu computador, protege el archivo:

   ```bash
   chmod 400 kafka-consumers.pem
   ```

3. Prueba la conexión:

   ```bash
   ssh -i kafka-consumers.pem ec2-user@IP_O_DNS_DE_LA_EC2
   ```

4. En GitHub, ve a **Settings** → **Secrets and variables** → **Actions** y crea:

| Secret | Valor |
|---|---|
| `EC2_HOST` | IP pública o DNS de la EC2 |
| `EC2_USER` | `ec2-user` |
| `EC2_SSH_KEY` | Contenido completo del archivo `.pem`, incluyendo `-----BEGIN...` y `-----END...` |

> `EC2_SSH_KEY` es la **clave privada**, no el nombre del key pair. Nunca la subas al repositorio.

5. El security group de la EC2 debe permitir SSH (puerto 22). Los runners de GitHub Actions usan IPs dinámicas, así que para la actividad normalmente se abre a `0.0.0.0/0`; en producción conviene restringirlo.

#### C. Permisos para invocar la Lambda

Las credenciales que están en el `.env` de la EC2 necesitan permiso para invocar la Lambda. Si usas un usuario o rol propio, agrégale una política como esta (reemplaza la cuenta y la región):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-1:123456789012:function:guardar-log-dynamodb"
    }
  ]
}
```

- Si usas **AWS Academy**, las credenciales del Learner Lab normalmente ya tienen permisos amplios; si no, revisa el rol del laboratorio.
- Si la EC2 usa un **rol de instancia**, adjunta la política al rol y puedes borrar las tres variables `AWS_*` del `.env`.
- Recuerda que el **rol de ejecución de la Lambda** (no el de la EC2) es el que necesita `dynamodb:PutItem`, como se explicó en la sección 6.2.

#### D. Kafka debe ser accesible

- Si Kafka está en la misma EC2, usa `KAFKA_BROKERS=localhost:9092`.
- Si Kafka está en otra máquina, usa su IP o DNS y abre el puerto `9092` en el security group.
- El broker debe anunciar una dirección que la EC2 pueda alcanzar (`advertised.listeners`). Si el broker anuncia `localhost`, la EC2 no podrá conectarse.
- El topic debe existir. Puedes crearlo con:

  ```bash
  kafka-topics.sh --bootstrap-server IP_DEL_BROKER:9092 \
    --create --topic compras-topic --partitions 1 --replication-factor 1
  ```

> El código usa Kafka sin autenticación (PLAINTEXT). Si usas Confluent Cloud u otro Kafka con SASL/SSL, hay que agregar `sasl` y `ssl` a la configuración del cliente.

### 13.10. Cómo probar los 3 consumers

1. Asegúrate de que los consumers estén corriendo:

   ```bash
   pm2 status
   pm2 logs consumers
   ```

2. Abre una consola y escribe un mensaje en el topic con el productor de Kafka:

   ```bash
   kafka-console-producer.sh --bootstrap-server IP_DEL_BROKER:9092 \
     --topic compras-topic
   ```

3. Pega este JSON y presiona Enter:

   ```json
   {"tipo":"compra","compraId":"C-1001","monto":25000,"cliente":"cristopher"}
   ```

4. En los logs de PM2 deberías ver mensajes de los tres grupos, por ejemplo:

   ```text
   [logs-dynamodb] Mensaje recibido en compras-topic (partition=0, offset=0)
   [LOGS DYNAMO] Lambda invocada (status 200)
   [LOGS DYNAMO] Respuesta: {"statusCode":200,"body":"Log guardado"}
   [validacion-pago] Mensaje recibido en compras-topic (partition=0, offset=0)
   [VALIDACION PAGO] Pago aprobado
   [VALIDACION PAGO] Monto validado: 25000
   [comprobante-pago] Mensaje recibido en compras-topic (partition=0, offset=0)
   [COMPROBANTE PAGO] Boleta generada
   [COMPROBANTE PAGO] Código de boleta: A1B2C3
   ```

   El orden exacto puede variar porque los tres consumers trabajan en paralelo.

5. Verifica en **DynamoDB** → `LogsCompras` que apareció el registro con `id`, `fecha` y `evento`.

6. Prueba un pago inválido:

   ```json
   {"tipo":"compra","compraId":"C-1002","monto":0}
   ```

   Deberías ver:

   ```text
   [VALIDACION PAGO] Pago rechazado
   [VALIDACION PAGO] Monto inválido: 0
   ```

7. Para detener los consumers:

   ```bash
   pm2 stop consumers
   ```

> Si acabas de crear los grupos y el topic ya tenía mensajes, `fromBeginning: true` hará que los lean todos. Si no quieres reprocesar, cambia a `fromBeginning: false` y reinicia.

### 13.11. Errores comunes de los consumers

| Error | Causa probable | Solución |
|---|---|---|
| `KafkaJSConnectionError` / `ECONNREFUSED` | El broker no es accesible | Revisar IP, puerto 9092, security group y `advertised.listeners` |
| `KafkaJSNumberOfRetriesExceeded` | Kafka no responde | Verificar que Kafka esté encendido y que `KAFKA_BROKERS` sea correcto |
| `Topic not found` | El topic no existe | Crear `compras-topic` |
| `CredentialsProviderError` | Faltan credenciales de AWS | Completar `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY` en `.env` |
| `ExpiredToken` | Las credenciales de AWS Academy caducaron | Copiar credenciales nuevas al `.env` y reiniciar PM2 |
| `AccessDeniedException` al invocar la Lambda | Las credenciales no tienen `lambda:InvokeFunction` | Agregar la política de la sección 13.9 C |
| `ResourceNotFoundException` al invocar | Nombre o región de la Lambda incorrectos | Revisar `LAMBDA_LOGS_NAME` y `AWS_REGION` |
| `Cannot find module 'kafkajs'` | No se ejecutó `npm install` | Entrar a `consumers` y ejecutar `npm install` |
| `pm2: command not found` | PM2 no está instalado o no está en el PATH | `sudo npm install -g pm2` y verificar el PATH |
| `git pull` falla por autenticación | La EC2 no tiene acceso al repositorio | Configurar una deploy key o un token de solo lectura |
| `Permission denied (publickey)` | La clave SSH no coincide | Revisar `EC2_SSH_KEY` y el key pair de la EC2 |
| Se procesan mensajes antiguos | `fromBeginning: true` en un grupo nuevo | Es esperado; usar `false` si no se quieren reprocesar |

### 13.12. Puntos clave para la defensa oral (Parte II)

1. **Tres consumers, tres grupos**: cada `groupId` recibe una copia de todos los mensajes. Si compartieran grupo, Kafka repartiría los mensajes y no todos harían su tarea.
2. **Un solo proceso**: los tres consumers viven en `consumers/index.js` y PM2 mantiene el proceso con el nombre `consumers`.
3. **Consumer de logs**: usa `LambdaClient` y `InvokeCommand` para invocar `guardar-log-dynamodb` con `InvocationType: "RequestResponse"` y el JSON del mensaje.
4. **Consumer de validación**: convierte `monto` con `Number()` y aprueba solo si es mayor que 0.
5. **Consumer de comprobante**: genera un código aleatorio con `Math.random()` y lo imprime.
6. **`fromBeginning: true`**: permite leer los mensajes existentes la primera vez que se crea cada grupo; en producción se usaría `false`.
7. **Manejo de errores**: cada mensaje tiene su `try/catch`; si falla uno, el consumer sigue funcionando. En producción se podría relanzar el error o usar una DLQ.
8. **Variables de entorno**: `dotenv` carga `.env`; las credenciales nunca están en el código ni en GitHub.
9. **CI/CD con filtro de rutas**: `deploy-consumers.yml` solo se ejecuta si cambia `consumers/**`, y despliega por SSH en la EC2.
10. **PM2**: `pm2 restart consumers || pm2 start index.js --name consumers` reinicia el proceso si existe o lo crea la primera vez.
11. **Separación de permisos**: la EC2 necesita `lambda:InvokeFunction`; la Lambda necesita `dynamodb:PutItem`. Son dos roles/credenciales distintos.
12. **Mejoras para producción**: usar `fromBeginning: false`, autenticación SASL/SSL si el broker la exige, UUID para la boleta, reintentos/DLQ y OIDC en lugar de claves de larga duración.
