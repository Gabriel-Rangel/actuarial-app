# Estrutura da Aplicação

Este diretório contém o código fonte da aplicação Flask que é executada no Databricks Apps.

## Arquivos Principais

*   **`app.py`**: O ponto de entrada da aplicação. Configura o servidor Flask, define as rotas, gerencia a autenticação e a injeção de URLs para os componentes incorporados.
*   **`app.yaml`**: Arquivo de configuração de implantação. Define o comando de execução e as variáveis de ambiente necessárias para conectar aos Dashboards e Genie Spaces.
*   **`genie_embedding.py`**: Módulo auxiliar responsável por interagir com a API do Genie e gerar as URLs de incorporação seguras.
*   **`requirements.txt`**: Lista as dependências Python necessárias (Flask, requests, etc.).

## Diretórios

*   **`templates/`**: Contém os templates HTML (Jinja2) que definem a interface do usuário.
    *   `layout.html`: Define a estrutura base da página (cabeçalho, barra lateral, rodapé).
    *   `analytics.html`: Template para exibir os Dashboards Lakeview.
    *   `genie.html`: Template para exibir a interface do Genie.
*   **`static/`**: Contém arquivos estáticos.
    *   `css/styles.css`: Estilos personalizados da aplicação.
    *   `js/scripts.js`: Scripts para interatividade do frontend.

## Configuração

A aplicação é configurada através de variáveis de ambiente definidas no `app.yaml`. Estas variáveis são preenchidas automaticamente pelo script de setup (`_setup.ipynb`) com os IDs dos recursos criados no seu workspace.

*   `DATABRICKS_HOST`: URL do workspace Databricks.
*   `DATABRICKS_DASHBOARD_*_ID`: IDs dos Dashboards Lakeview.
*   `DATABRICKS_GENIE_SPACE_*_ID`: IDs dos Genie Spaces.