import Event from './event.model';
export default class Window {
  id: string;
  startedTime: number;
  closedTime: number;
  events: Event[];
  isFreeze: boolean;

  private process() {
    console.log(`Processing window: ${this.id} count ${this.events.length}`);
    this.events.forEach((event) => {
      console.log(`Event: ${JSON.stringify(event)}`);
    });
  }
  close() {
    this.closedTime = Date.now();
    if (this.events.length) {
      this.process();
    }
  }
}
