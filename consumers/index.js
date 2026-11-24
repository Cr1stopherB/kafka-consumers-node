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
