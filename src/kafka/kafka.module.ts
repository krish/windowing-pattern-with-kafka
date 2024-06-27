import { Module } from '@nestjs/common';
import { KafkaService } from './kafka.service';
import { SchedullerService } from './scheduller/scheduller.service';
import { KafkaController } from './kafka.controller';

@Module({
  providers: [KafkaService, SchedullerService],
  controllers: [KafkaController],
})
export class KafkaModule {}
