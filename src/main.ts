import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  process.env.KAFKA_BROKERS = 'localhost:9092';

  const app = await NestFactory.create(AppModule);
  await app.listen(3000);
}
bootstrap();
