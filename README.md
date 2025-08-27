🩺 OperaInsight

Automação de relatórios de saúde com Python + Excel (VBA).
Este projeto integra dados fictícios do SigSaúde e Operadoras, aplica regras de qualidade e gera relatórios prontos para análise, com tabelas dinâmicas, rankings e gráficos.
Tudo pode ser atualizado diretamente pelo Excel através de botões que executam scripts Python.

🚀 Funcionalidades

📥 Importação automática de arquivos CSV/TXT (detecta separador e encoding).

🧹 Limpeza de dados:

Conversão de datas.

Números negativos → 0.

Remoção de duplicados.

Preenchimento de valores ausentes.

🔗 Enriquecimento: merge/PROCV entre ClienteId e tabela de clientes (DimClientes).

📊 Relatórios no Excel:

Aba Resumo: visão consolidada com gráficos.

Aba Dados: base limpa e formatada.

Aba Rankings: top Operadoras e Procedimentos.

Aba Parametros: filtros dinâmicos (período, UFs, categorias).

Aba Auditoria: registro de execução (contagens, erros, removidos).

🖱️ Botões VBA no Excel para executar:

Atualizar Tudo → reprocessa os dados.

Gerar Gráficos → atualiza apenas os gráficos.

Gerar Rankings → recalcula apenas os rankings.

📝 Logs: registrados em logs/agent.log + aba Auditoria.

📂 Estrutura do Projeto
relatorio_saude/
  Relatorio_Saude.xlsm        # Planilha final com macros e botões
  config.yaml                 # Configurações globais
  logs/
    agent.log
  scripts/
    agent.py                  # Script Python principal
  vba/
    ModuleExcelAgent.bas      # Macros em VBA
  sample_data/
    sigsaude/...
    operadoras/...
    dim_clientes.csv

⚙️ Tecnologias Usadas

Python

pandas → manipulação de dados

openpyxl → escrita e formatação no Excel

xlwings → integração Excel ⇄ Python

pyyaml → leitura de configurações

Excel (VBA)

Macros para chamar Python via Shell/xlwings

Botões interativos na aba Resumo

Funções SOMA, SOMASE, SE, PROCV

🔧 Instalação

Clone este repositório:

git clone https://github.com/Nicolas-Vilcher-Garrido/relatorio_saude.git
cd relatorio_saude


Instale as dependências:

pip install pandas openpyxl xlwings pyyaml


Rode o script manualmente (opcional):

python scripts/agent.py

🖱️ Uso no Excel

Abra o arquivo Relatorio_Saude.xlsm.

Importe o módulo VBA (vba/ModuleExcelAgent.bas).

Insira botões na aba Resumo e atribua as macros:

Botao_AtualizarTudo

Botao_GerarGraficos

Botao_GerarRankings

Clique nos botões para atualizar relatórios automaticamente.

⚙️ Configuração

Arquivo config.yaml:

entrada:
  sigsaude_dir: "./sample_data/sigsaude"
  operadoras_dir: "./sample_data/operadoras"

parametros:
  periodo_inicio: "2024-01-01"
  periodo_fim: "2025-12-31"
  moeda: "R$"
  uf_incluir: ["SP","RJ","MG"]
  categoria_incluir: ["A","B","C"]

chaves:
  cliente_id: "ClienteId"
  dim_clientes: "./sample_data/dim_clientes.csv"

saida:
  relatorio_xlsm: "./Relatorio_Saude.xlsm"
  auditoria_sheet: "Auditoria"
  logs_dir: "./logs"

📊 Exemplos de Saída

Gráfico de Linha: Receita por mês.

Gráfico de Colunas: Receita por Operadora.

Gráfico de Pizza: % Receita por Procedimento.

Rankings: principais Operadoras e Procedimentos.

📝 Próximos Passos

🔄 Automatizar execução (Windows Task Scheduler / cron).

📈 Expandir análises (faixa etária, região, categoria).

⚡ Implementar atualizações incrementais para grandes volumes de dados.

📜 Licença

Este projeto é open-source sob a licença MIT.

