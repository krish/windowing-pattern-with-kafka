import { Controller, Get } from '@nestjs/common';
import { KafkaService } from './kafka.service';

@Controller('kafka')
export class KafkaController {
  constructor(private kafkaService: KafkaService) {}

  @Get('start')
  startProducingMessages() {
    this.kafkaService.startProducingMessages();
    return { message: 'Started producing messages' };
  }

  @Get('stop')
  stopProducingMessages() {
    this.kafkaService.stopProducingMessages();
    return { message: 'Stopped producing messages' };
  }
}
