-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1. Sobre o conjunto de dados
-- MAGIC   
-- MAGIC Há uma falta de conjuntos de dados públicos disponíveis sobre serviços financeiros e especialmente no domínio emergente de transações de dinheiro móvel. Conjuntos de dados financeiros são importantes para muitos pesquisadores e, em particular, para nós que realizamos pesquisas no domínio da detecção de fraudes. Parte do problema é a natureza intrinsecamente privada das transações financeiras, o que leva à inexistência de conjuntos de dados públicos disponíveis.
-- MAGIC
-- MAGIC Apresentamos um conjunto de dados sintéticos gerado usando o simulador chamado PaySim como uma abordagem para tal problema. O PaySim usa dados agregados do conjunto de dados privado para gerar um conjunto de dados sintéticos que se assemelha à operação normal de transações e injeta comportamento malicioso para posteriormente avaliar o desempenho dos métodos de detecção de fraude.
-- MAGIC
-- MAGIC O PaySim simula transações de dinheiro móvel com base em uma amostra de transações reais extraídas de um mês de registros financeiros de um serviço de dinheiro móvel implementado em um país africano. Os registros originais foram fornecidos por uma empresa multinacional, que é a provedora do serviço financeiro móvel que atualmente está em execução em mais de 14 países ao redor do mundo.
-- MAGIC
-- MAGIC Este conjunto de dados sintéticos é reduzido em 1/4 do conjunto de dados original e é criado apenas para o Kaggle, sendo assim essa análise de dados foi realizada a partir de um conjunto de dados coletados no site 'www.kaggle.com', com o tema "Synthetic Financial Datasets For Fraud Detection".

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1.1 Dicionário de Dados
-- MAGIC
-- MAGIC step - mapeia uma unidade de tempo no mundo real. Neste caso, 1 step é 1 hora de tempo. Total de steps 744 (simulação de 30 dias).
-- MAGIC
-- MAGIC type - ENTRADA, SAÍDA, DÉBITO, PAGAMENTO e TRANSFERÊNCIA.
-- MAGIC
-- MAGIC amount -
-- MAGIC valor da transação em moeda local.
-- MAGIC
-- MAGIC nameOrig - cliente que iniciou a transação
-- MAGIC
-- MAGIC oldbalanceOrg - saldo inicial antes da transação
-- MAGIC
-- MAGIC newbalanceOrig - novo saldo após a transação.
-- MAGIC
-- MAGIC nameDest - cliente que é o destinatário da transação
-- MAGIC
-- MAGIC oldbalanceDest - destinatário do saldo inicial antes da transação. Note que não há informações para clientes que começam com M (Merchants).
-- MAGIC
-- MAGIC newbalanceDest - novo destinatário do saldo após a transação. Note que não há informações para clientes que começam com M (Merchants).
-- MAGIC
-- MAGIC isFraud - Estas são as transações feitas pelos agentes fraudulentos dentro da simulação. Neste conjunto de dados específico, o comportamento fraudulento dos agentes visa lucrar tomando o controle das contas dos clientes e tentar esvaziar os fundos transferindo para outra conta e, em seguida, sacando do sistema.
-- MAGIC
-- MAGIC isFlaggedFraud - O modelo de negócios visa controlar transferências massivas de uma conta para outra e sinaliza tentativas ilegais. Uma tentativa ilegal neste conjunto de dados é uma tentativa de transferir mais de 200.000 em uma única transação.
-- MAGIC
-- MAGIC **OBSERVAÇÃO: As transações detectadas como fraude são canceladas, portanto, para detecção de fraude, essas colunas (oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest) não devem ser usadas.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Importação do CSV
-- MAGIC   Nao utilizaremos nesse primeiro momento a conexao com os bancos de dados, visto que o material e apenas para um estudo breve e de tamanho consideravel reduzido, entao importamos o csv direto para dentro da plataforma do databricks e utilizaremos SQL para as queries.
-- MAGIC
-- MAGIC   Sendo assim, seguimos o passo de criar o cluster em versao "12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)", na aba "Compute".
-- MAGIC
-- MAGIC   E tambem importamos o dataset de forma manual para dentro do WorkSpace aqui da plataforma do DataBricks para utilização.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Entendendo a estrutura

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.1 Inspeção do esquema e tipo dos dados

-- COMMAND ----------

-- Considerando que nessa análise utilizaremos apenas um conjunto de dados, aqui ja conseguimos identificar os tipos dos dados e se os mesmos possuem valores nulos, para agregar a informação do dicionário dos dados. 

DESCRIBE financialfraud

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.2 Visualizando as 10 primeiras linhas da tabela

-- COMMAND ----------

-- Essas são as 10 primeiras linhas do conjunto de dados

SELECT *
FROM financialfraud
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.3 Distribuição dos tipos de pagamento

-- COMMAND ----------

-- Contagem por tipos de pagamento totais

SELECT DISTINCT type, COUNT(*) 
FROM financialfraud
GROUP BY type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Limpeza e preparaçao dos dados
-- MAGIC
-- MAGIC O foco nessa etapa seria:
-- MAGIC * 3.1 Identificar valores nulos e inconsistências;
-- MAGIC * 3.2 Contar registros totais e verificar a distribuição de classes (fraude vs. não fraude);
-- MAGIC * 3.3 Verificar se há transações duplicadas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.1 Identificar valores nulos e inconsistências

-- COMMAND ----------

-- Contar valores nulos se houver
SELECT 
    COUNT(*) AS totalRows,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS nullAmounts,
    SUM(CASE WHEN type IS NULL THEN 1 ELSE 0 END) AS nullTypes,
    SUM(CASE WHEN nameOrig IS NULL THEN 1 ELSE 0 END) AS nullNameOrig,
    SUM(CASE WHEN nameDest IS NULL THEN 1 ELSE 0 END) AS nullNameDest
FROM financialfraud;

-- COMMAND ----------

-- Dupla checagem de valores nulos com codigo ja pre pronto utilizado em outros estudos.
SELECT 
    SUM(CASE WHEN step IS NULL THEN 1 ELSE 0 END) AS null_step,
    SUM(CASE WHEN type IS NULL THEN 1 ELSE 0 END) AS null_type,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amount,
    SUM(CASE WHEN nameOrig IS NULL THEN 1 ELSE 0 END) AS null_nameOrig,
    SUM(CASE WHEN oldbalanceOrg IS NULL THEN 1 ELSE 0 END) AS null_oldbalanceOrg,
    SUM(CASE WHEN newbalanceOrig IS NULL THEN 1 ELSE 0 END) AS null_newbalanceOrig,
    SUM(CASE WHEN nameDest IS NULL THEN 1 ELSE 0 END) AS null_nameDest,
    SUM(CASE WHEN oldbalanceDest IS NULL THEN 1 ELSE 0 END) AS null_oldbalanceDest,
    SUM(CASE WHEN newbalanceDest IS NULL THEN 1 ELSE 0 END) AS null_newbalanceDest,
    SUM(CASE WHEN isFraud IS NULL THEN 1 ELSE 0 END) AS null_isFraud,
    SUM(CASE WHEN isFlaggedFraud IS NULL THEN 1 ELSE 0 END) AS null_isFlaggedFraud
FROM financialfraud;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.2 Contar registros totais
-- MAGIC

-- COMMAND ----------

-- Total de transações

SELECT COUNT(*) AS total_transacoes FROM financialfraud;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.3 distribuição de classes (fraude vs. não fraude)

-- COMMAND ----------

-- Distribuição entre fraudes e não fraudes, sendo 1 para "Sim" e 0 para "Não"

SELECT isFraud, COUNT(*) AS quantidade
FROM financialfraud
GROUP BY isFraud;


-- COMMAND ----------

SELECT isFraud
FROM financialfraud
WHERE isFraud = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.4 Verificar se há transações duplicadas
-- MAGIC

-- COMMAND ----------

-- Essa consulta identifica quantas transações têm valores repetidos em múltiplas colunas-chave.

SELECT 
    step,
    type,
    amount,
    nameOrig,
    nameDest,
    COUNT(*) AS qtd_duplicadas
FROM financialfraud
GROUP BY step, type, amount, nameOrig, nameDest
HAVING COUNT(*) > 1
ORDER BY qtd_duplicadas DESC;

-- COMMAND ----------

-- Se quisermos ver os registros exatos que estão duplicados, usamos esta query:

SELECT * 
FROM financialfraud
WHERE (step, type, amount, nameOrig, nameDest) IN (
    SELECT step, type, amount, nameOrig, nameDest
    FROM financialfraud
    GROUP BY step, type, amount, nameOrig, nameDest
    HAVING COUNT(*) > 1
)
ORDER BY step, nameOrig;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5. Análise exploratória inicial

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5.1 Primeiros registros do conjunto

-- COMMAND ----------

-- Verificando os primeiros registros e conhecendo a planilha
SELECT *
FROM financialfraud
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5.2 Visão Geral das Transações

-- COMMAND ----------

-- Contagem de transações por tipo (Grafico disponivel)
SELECT 
    type, 
    COUNT(*) AS totalTransactions, 
    ROUND(SUM(amount), 2) AS totalAmount
FROM financialfraud
GROUP BY type
ORDER BY totalTransactions DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5.3 Distribuição de Fraudes

-- COMMAND ----------

-- Contagem de transações normais, fraudulentas e sinalizadas. (Grafico disponivel)
SELECT 
    CASE 
        WHEN isFraud = 1 THEN 'Fraudulent'
        WHEN isFlaggedFraud = 1 THEN 'Flagged as Fraud'
        ELSE 'Normal'
    END AS transactionStatus,
    COUNT(*) AS transactionCount
FROM financialfraud
GROUP BY transactionStatus;

-- A porcentagem de transacoes normais e de 99,9% (6.354.407) quanto aos 0,01% (8213) das fraudulentas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5.4 Fraudes por tipo de transação

-- COMMAND ----------

-- Distribuição de fraudes por tipo de transação
SELECT 
    type,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraudCount,
    SUM(CASE WHEN isFlaggedFraud = 1 THEN 1 ELSE 0 END) AS flaggedCount,
    COUNT(*) AS totalTransactions
FROM financialfraud
GROUP BY type
ORDER BY fraudCount DESC;

-- Aqui podemos identificar que a maioria das transaçoes provenientes de fraude sao advindas dos meios de CASH_OUT e TRANSFER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5.5 Valor médio e máximo das transações
-- MAGIC

-- COMMAND ----------

SELECT 
    type,
    ROUND(AVG(amount), 2) AS average_value,
    ROUND(MAX(amount), 2) AS max_value
FROM 
    financialfraud
GROUP BY type;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6. Identificação de Padrões Suspeitos

-- COMMAND ----------

-- Distribuição entre transações sinalizadas como suspeitas:

SELECT isFlaggedFraud, COUNT(*) AS quantidade
FROM financialfraud
GROUP BY isFlaggedFraud;

-- COMMAND ----------

-- Para entender a distribuição das operações:

SELECT type, COUNT(*) AS total
FROM financialfraud
GROUP BY type
ORDER BY total DESC;


-- COMMAND ----------

-- Por regra, os saldos não deveriam ser negativos. Vamos identificar transações problemáticas:

SELECT * 
FROM financialfraud 
WHERE newbalanceOrig < 0 OR newbalanceDest < 0;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6.1 Transações de Alto Valor (Possível Lavagem de Dinheiro)

-- COMMAND ----------

-- Tentando localizar possiveis fraudes que ultrapassam 3 vezes o valor do desvio padrao considerando tambem a media, indicando um alerta. (Grafico disponivel)

SELECT * 
FROM financialfraud 
WHERE amount > (SELECT AVG(amount) + 3 * STDDEV(amount) FROM financialfraud)
ORDER BY amount DESC;

-- Evidenciando a diferença existente no TRANSFER e CASH_OUT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6.2 Contas de Alta Frequência

-- COMMAND ----------

-- Identificando os usuarios que fazem transaçoes acima da media.

SELECT nameOrig, COUNT(*) AS total_transactions
FROM financialfraud
GROUP BY nameOrig
HAVING COUNT(*) > (SELECT AVG(trans_count) + 3 * STDDEV(trans_count) 
                   FROM (SELECT nameOrig, COUNT(*) AS trans_count 
                         FROM financialfraud 
                         GROUP BY nameOrig) AS subquery)
ORDER BY total_transactions DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6.3 Saldos negativos apos transaçoes

-- COMMAND ----------

-- A ideia aqui seria de identificar usuários que ficaram com saldo negativo após uma transação, o que pode indicar comportamento fraudulento ou erro no sistema.

SELECT * 
FROM financialfraud
WHERE newbalanceOrig < 0
ORDER BY newbalanceOrig ASC;

-- No caso em questao nao foi localizado nenhum erro ou valores negativos.


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 7. Conclusão

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7.1 A análise revelou os seguintes insights:
-- MAGIC
-- MAGIC - **Fraudes são predominantes em tipos específicos de transação:**
-- MAGIC
-- MAGIC > As transações TRANSFER e CASH-OUT concentram quase todas as fraudes. Isso sugere que esses tipos de operações são usados por agentes maliciosos para escoar fundos rapidamente.
-- MAGIC
-- MAGIC - **Transações de alto valor apresentam maior risco:**
-- MAGIC
-- MAGIC > Valores acima de 200.000 foram sinalizados como potencialmente suspeitos, sugerindo a possibilidade de lavagem de dinheiro.
-- MAGIC
-- MAGIC - **Perfis de contas anômalos:**
-- MAGIC
-- MAGIC > Algumas contas realizaram um número desproporcional de transações fraudulentas, o que pode indicar contas controladas por fraudadores.
-- MAGIC
-- MAGIC - **Valores médios elevados em fraudes:**
-- MAGIC
-- MAGIC > Transações fraudulentas têm valores médios mais altos, enquanto transações normais apresentam maior dispersão.
-- MAGIC
-- MAGIC - **Próximos Passos:**
-- MAGIC
-- MAGIC > Em minha opiniao, para uma próxima análise, devemos aprofundar a investigação nos usuários identificados como suspeitos e cruzar esses achados com outras variáveis, como geolocalização e tempo entre transações, para refinar ainda mais o modelo de detecção de fraudes e ja criarmos um alerta para certos usuário, ou ate mesmo ja criar uma barreira de cancelamento automático. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7.2 Recomendações
-- MAGIC
-- MAGIC 🔶 Aprimorar regras de monitoramento em tempo real para detectar transações fora do padrão e ja realizar o devido bloqueio e ou confirmaçao.
-- MAGIC
-- MAGIC 🔶 Aplicar limites progressivos para transações de novos usuários ou contas suspeitas, levando em consideraçao os valores e tempo por cada transaçao.
-- MAGIC
-- MAGIC 🔶 Revisar políticas de saldo negativo para evitar que usuários explorem falhas do sistema, colocando sempre um alerta no sistema para quando ocorrer uma conta com valor negativo.
-- MAGIC
-- MAGIC 🔶 Utilizar aprendizado de máquina para detectar padrões de fraude de forma mais eficiente.
-- MAGIC
