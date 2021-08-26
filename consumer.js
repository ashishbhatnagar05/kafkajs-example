const { kafka, Kafka } = require("kafkajs")
async function run() {
    const kafka = new Kafka(
        {
            "clientId": "myapp",
            "brokers": ["localhost:29092"]
        }
    )
    const consumer = kafka.consumer({"groupId":"test1"})

    try {
        console.log("Connecting.......")
        await consumer.connect()
        console.log("Connected.......")
        
        await consumer.subscribe({
            "topic": "Users",
            "fromBeginning": true
        })
        console.log("Connected Successfully.......")

        await consumer.run({
            "eachMessage":async result=>{
                console.log(`Received message ${result.message.value} on partition  ${result.partition}`)
            }
        })

    }

    catch (ex) {
        console.error(`Error: ${ex}`)
    }
}

run()