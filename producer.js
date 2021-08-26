const { kafka, Kafka } = require("kafkajs")
const msg = process.argv[2];
async function run() {
    const kafka = new Kafka(
        {
            "clientId": "myapp",
            "brokers": ["localhost:29092"]
        }
    )
    const producer = kafka.producer()

    try {
        console.log("Connecting.......")
        await producer.connect()
        console.log("Connected.......")

        const partition = msg[0] < "N" ? 0 : 1
        const result = await producer.send({
            "topic": "Users",
            "messages": [
                {
                    "value": msg,
                    "partition": partition
                }
            ]
        })
        console.log(`Result: ${JSON.stringify(result)}`)
        console.log("Sent Successfully.......")

    }

    catch (ex) {
        console.error(`Error: ${ex}`)
    }
    finally {
        producer.disconnect()
        process.exit(0)
    }
}

run()