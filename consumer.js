const amqp=require("amqplib")
const data = require("./data.json")
const redis = require("redis")

const client=redis.createClient();
const queueName = process.argv[2] || "jobsQueue"
console.log(queueName)
connect_rabbitmq();

async function connect_rabbitmq(){
   try {
    const connection =await amqp.connect("amqp://localhost:5672");
    const channel = await connection.createChannel();
    const assertion = await channel.assertQueue(queueName)

    //mesaj alınması
    console.log("mesaj bekleniyor")
    channel.consume(queueName,(message)=>{
        const messageInfo = JSON.parse(message.content.toString());
        console.log(messageInfo)
        //{description:12}
        const userInfo=data.find(u=>u.id==messageInfo.description)
        if(userInfo){
            console.log("işlem kayıt",userInfo)

            client.set(`user_${userInfo.id}`,
            JSON.stringify(userInfo),
            (err,status)=>{
                if(!err){
                    console.log("Status",status);
                    channel.ack(message)
                }
            }
            );
        }
        
        //=========================================================================
        // console.log("message",message.content.toString());
        // channel.ack(message)
        //=========================================================================
    })
   } catch (error) {
    console.log("error",error)
   }
}