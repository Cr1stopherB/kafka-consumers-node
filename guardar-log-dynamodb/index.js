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
