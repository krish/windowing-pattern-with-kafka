import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Kafka, Consumer, Producer } from 'kafkajs';
import Window from './window.model';
import Event from './event.model';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private KAFKA_TOPIC = 'krish-windowing';
  private KAFKA_GROUP_ID: string = 'windowing-consumer';
  private intervalId: NodeJS.Timeout;
  private kafka: Kafka;
  private consumer: Consumer;
  private producer: Producer;
  private readonly topic = this.KAFKA_TOPIC;
  private isPaused = false;
  private lastOffset;

  constructor() {
    this.kafka = new Kafka({
      brokers: process.env.KAFKA_BROKERS.split(','),
    });
    this.consumer = this.kafka.consumer({
      groupId: this.KAFKA_GROUP_ID,
    });
    this.producer = this.kafka.producer();
  }

  async onModuleInit() {
    await this.consumer.connect();
    await this.producer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: false });

    this.consumeMessages();
    //this.produceMessages();
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
    await this.producer.disconnect();
  }

  async consumeMessages() {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const currentDate = new Date();

        try {
          const messageTimestamp = new Date(
            message.value.toString().split('Message at ')[1],
          );
          const timeDifference =
            (currentDate.getTime() - messageTimestamp.getTime()) / 1000;

          console.log(
            `Consumed message: offset: ${message.offset} diff: ${timeDifference}`,
          );

          if (timeDifference < 60 && !this.isPaused) {
            console.log('Pausing consumer...');
            this.lastOffset = message.offset;
            this.consumer.pause([{ topic: this.topic }]);
            this.isPaused = true;
            this.closeCurrentWindow();
          } else {
            if (!this.isWindowFrozen()) {
              const event: Event = {
                eventTime: messageTimestamp,
                currentTime: currentDate,
                timeDiff: timeDifference,
                offset: message.offset,
              };
              this.addEventToCurrentWindow(event);
            } else {
              console.warn(
                'Opppps... window is forzen. setting last offset------------------------------------------',
              );
              this.lastOffset = message.offset;
              this.consumer.pause([{ topic: this.topic }]);
              this.isPaused = true;
            }
          }
        } catch (error) {
          console.error('Error processing message:', error);
        }
      },
    });
  }

  async startProducingMessages() {
    if (!this.intervalId) {
      this.intervalId = setInterval(async () => {
        const message = { value: `Message at ${new Date().toISOString()}` };
        const recordMeta = await this.producer.send({
          topic: this.topic,
          messages: [message],
        });

        /*   console.log(
          `Produced message: ${recordMeta[0].baseOffset} - ${message.value}`,
        ); */
      }, 100);
    }
  }

  stopProducingMessages() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
      console.log('Stopped producing messages');
    }
  }

  async resumeConsumer() {
    if (this.isPaused) {
      console.log('Resuming consumer...');
      if (this.lastOffset !== null) {
        // Seek to the last processed offset
        this.consumer.seek({
          topic: this.topic,
          partition: 0,
          offset: this.lastOffset,
        });
      }
      this.consumer.resume([{ topic: this.topic }]);
      this.isPaused = false;
    }
  }

  addEventToCurrentWindow(event: Event) {
    // This method should be implemented by the SchedulerService
    throw new Error('Method not implemented.');
  }

  closeCurrentWindow() {
    // This method should be implemented by the SchedulerService
    throw new Error('Method not implemented.');
  }
  isWindowFrozen(): boolean {
    // This method should be implemented by the SchedulerService
    throw new Error('Method not implemented.');
  }
}
