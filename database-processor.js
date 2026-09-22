//type of consumer
import {kafkaClient} from './kafka-client.js'

const PORT=process.env.PORT??8001
const LOCATION_TOPIC='location-updates'

async function init(){
            // Use a separate group so this processor receives every location event.
        const kafkaConsumer=kafkaClient.consumer({groupId:`database-processor-${PORT}`})
    await kafkaConsumer.connect();
    await kafkaConsumer.subscribe({
        topics:[LOCATION_TOPIC],
        fromBeginning:true
    })

    // Broadcast Kafka messages to all connected browsers.
    kafkaConsumer.run({
        eachMessage:async({topic,partition,message,heartbeat})=>{
            const data=JSON.parse(message.value.toString())
            console.log("insert into db location ",data)
            
            await heartbeat();
        }
    })
}
init()