const { kafka, Kafka } = require("kafkajs")

async function run() {
    const kafka = new Kafka(
        {
            "clientId": "myapp",
            "brokers": ["localhost:29092"]
        }
    )
    const admin = kafka.admin()

    try {
        console.log("Connecting.......")
        await admin.connect()
        console.log("Connected.......")

        await admin.createTopics({
            "topics": [{
                "topic": "Users",
                "numPartitions": 2
            }]
        })
        console.log("Created Successfully.......")

    }

    catch (ex) {
        console.error(`Error: ${ex}`)
    }
    finally {
        admin.disconnect()
        process.exit(0)
    }
}

run()