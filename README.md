# Actuarial App

Este projeto é uma aplicação demonstrativa de análise atuarial construída sobre a plataforma **Databricks Apps**. A aplicação utiliza **Flask** para fornecer uma interface web personalizada que integra e incorpora:

*   **Databricks Dashboards (Lakeview)**: Para visualização de dados de projeções de Anuidades Variáveis (Variable Annuities) e estudos de incidência de Cuidados de Longo Prazo (LTC).
*   **Databricks Genie Spaces**: Para exploração de dados baseada em linguagem natural (NLQ).

## Como Implantar

Para configurar e implantar esta aplicação no seu ambiente Databricks, siga os passos abaixo:

### 1. Adicionar como Pasta Git (Git Folder)
1.  No seu Workspace Databricks, navegue até a seção **Workspace**.
2.  Selecione a pasta onde deseja clonar o projeto (ex: `Users/<seu-usuario>`).
3.  Clique em **Create** > **Git folder**.
4.  Insira a URL deste repositório Git e clique em **Create Git folder**.

### 2. Executar o Setup
O processo de configuração é automatizado através de um notebook.

1.  Abra o notebook `_setup.ipynb` localizado na raiz do projeto.
2.  Execute todas as células do notebook ("Run All").

**O que o notebook `_setup.ipynb` faz?**
*   Cria o catálogo `actuarial_app_catalog` e os schemas necessários.
*   Carrega os dados CSV da pasta `data/` para tabelas Delta.
*   Aplica restrições de integridade (Primary Keys) nas tabelas.
*   Cria e publica os Dashboards automaticamente.
*   Cria os Genie Spaces configurados.
*   Atualiza automaticamente o arquivo de configuração `app/app.yaml` com os IDs dos recursos criados.
*   Cria e implanta a aplicação no Databricks Apps.

Após a execução bem-sucedida, o notebook exibirá o link para acessar a aplicação implantada.

## Estrutura do Projeto

Para detalhes técnicos sobre a estrutura do código da aplicação Flask, rotas e templates, consulte o arquivo [app/README.md](app/README.md).