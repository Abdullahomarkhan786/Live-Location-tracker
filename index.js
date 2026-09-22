import http from 'node:http'
import path from 'node:path'
import express from 'express'
import {Server} from 'socket.io'

import { kafkaClient } from './kafka-client.js'

const LOCATION_TOPIC='location-updates'

async function main(){
    const PORT=process.env.PORT??8000

    const app=express();
    const server=http.createServer(app)
    const io=new Server();

    app.use(express.static(path.resolve('./public')))


    // The producer sends each browser location update to Kafka.
    const kafkaProducer=kafkaClient.producer()
    await kafkaProducer.connect()

    // Each server instance gets its own consumer group so it can receive location events.
    const kafkaConsumer=kafkaClient.consumer({groupId:`socket-server-${PORT}`})
    await kafkaConsumer.connect();
    await kafkaConsumer.subscribe({
        topics:[LOCATION_TOPIC],
        fromBeginning:true
    })

    // Broadcast Kafka messages to all connected browsers.
    kafkaConsumer.run({
        eachMessage:async({topic,partition,message,heartbeat})=>{
            const data=JSON.parse(message.value.toString())
            console.log("kafka consumer data received",data)
            io.emit('server:location:updates',{
                id:data.id,
                latitude:data.latitude,
                longitude:data.longitude
            })
            await heartbeat();
        }
    })

    io.attach(server)

    io.on('connection',(socket)=>{
        console.log(`[Socket:${socket.id}]:Connected success`)
        // Receive the browser's latest coordinates. The server currently logs them.
        socket.on('client:location:update',async (locationData)=>{
            const {latitude,longitude}=locationData
            console.log(`[Socket:${socket.id}]:client:location:update`,locationData)
            // Forward the browser location to Kafka for other consumers.

            // Use the socket id so clients can identify each remote user.
            await kafkaProducer.send({topic:LOCATION_TOPIC,messages:[{
                key:socket.id,
                value:JSON.stringify({id:socket.id,latitude,longitude})

            }]})
        })
    })

    app.get('/healthy',(req,res)=>{
        return res.json({healthy:true})
    })

    server.listen(PORT,()=>console.log(`Server running ${PORT}`))

}
main()