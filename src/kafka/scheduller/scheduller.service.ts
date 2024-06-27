import { Injectable } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { KafkaService } from '../kafka.service';
import Window from '../window.model';
import Event from '../event.model';

@Injectable()
export class SchedullerService {
  private currentWindow: Window;

  constructor(private readonly kafkaService: KafkaService) {
    this.startNewWindow();
  }

  @Cron(CronExpression.EVERY_MINUTE)
  handleCron() {
    console.log('Starting new cycle --------------------------------');
    this.startNewWindow();
    this.kafkaService.resumeConsumer();
  }

  private startNewWindow() {
    if (this.currentWindow) {
      console.log(
        `previous window state : true  count: ${this.currentWindow.events.length}`,
      );
      this.currentWindow.isFreeze = true;
      this.closeCurrentWindow();
    }
    console.log(`previous window state : ${this.currentWindow}`);
    this.currentWindow = new Window();
    this.currentWindow.id = this.generateUUID();
    this.currentWindow.startedTime = Date.now();
    this.currentWindow.events = [];
    console.log('new Window opened');

    // Pass methods to KafkaService
    this.kafkaService.addEventToCurrentWindow =
      this.addEventToCurrentWindow.bind(this);
    this.kafkaService.closeCurrentWindow = this.closeCurrentWindow.bind(this);
    this.kafkaService.isWindowFrozen = this.isWindowFrozen.bind(this);
  }

  private addEventToCurrentWindow(event: Event) {
    this.currentWindow.events.push(event);
  }

  private closeCurrentWindow() {
    console.log('Closing window initiated');
    if (this.currentWindow) {
      this.currentWindow.close();
      this.currentWindow = undefined;
      console.log('window closed');
    }
  }

  private isWindowFrozen() {
    return this.currentWindow.isFreeze;
  }
  private generateUUID(): string {
    return Math.random().toString(36);
  }
}
