# Processo de ETL de dados em streaming
O arquivo fluxograma.pdf se refere ao fluxograma proposto para resolução do problema em questão.
De início, os dados são recebidos em arquivos .csv que são carregados com o mesmo formato para uma partição de armazenamento temporário dentro do Azure Blob Storage.
Após a carga dos dados para o Blob Storage, uma atividade do Data Factory detecta a adição de um novo arquivo e automaticamente faz o envio de todas as informações para o Azure Event Hubs (cada linha do arquivo csv será enviada como um evento distinto). Enquanto o Azure Event Hubs detectar e transmitir os eventos, um job no Data Bricks será responsável por ler em tempo real os dados transmitidos pelo Event Hubs e carregar os mesmos no armazenamento final, neste caso, o Delta Lake.
