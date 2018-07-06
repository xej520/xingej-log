package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"fmt"
	"time"
)

func main() {
	//获得一个配置实例
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll
	//做分区的负载均衡，这个策略是随机策略，也就是说，随机的分配到分区上
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	client, err := sarama.NewSyncProducer([]string{"192.168.1.201:9092"}, config)

	if err != nil {
		log.Info("producer close, err :\n", err)
		return
	}

	defer client.Close()

    for {
		msg := &sarama.ProducerMessage{}

		msg.Topic = "nginx_log"
		msg.Value = sarama.StringEncoder("this.is a good test, my message is good")

		pid, offset, err := client.SendMessage(msg)
		if err != nil {
			log.Info("send message failed :\n", err)
			return
		}
		fmt.Printf("pid:%d offset:%v\n", pid, offset)

		time.Sleep(time.Millisecond * 10)
		}


}





