# Clickbus_NextStope_SmartDestiny

Repositório com **códigos, dados e artefatos** do desafio FIAP (ClickBus). Aqui estão o **pipeline de coleta e tratamento na AWS** e os **notebooks/modelos de Machine Learning**.

Link do vídeo de apresentação pitch: https://www.youtube.com/watch?v=lQXnZpiyynI

## 🎯 Objetivos (Data & ML)
- **Decodificando o Comportamento do Cliente:** segmentar clientes por histórico de compras para orientar Growth (promoções, e-mail, push).
- **O Timing é Tudo:** prever se cada cliente comprará nos próximos **7 ou 30 dias** (classificação binária).
- **A Estrada à Frente:** prever o **par origem–destino** mais provável da próxima compra (multi-classe/recomendação).

## 🏗️ Arquitetura (AWS)
- **Armazenamento:** S3 no modelo **Bronze / Silver / Gold**
- **Processamento:** **AWS Glue** (Spark)
- **Orquestração:** **Step Functions**
- **Ingestão:** **Lambda** para MongoDB Atlas / RDS
- **Coleta (Scraping):** **ECS** com contêineres Docker

<img width="8804" height="3604" alt="image" src="https://github.com/user-attachments/assets/dfd63885-838e-4f8b-affd-c917c6a27ff6" />

