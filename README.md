# 🔗 Projeto de Engenharia de Dados 🔗

## Chegadas de Turistas Internacionais no Brasil

Este projeto é um pipeline de engenharia de dados, utilizando extrações do [Dataset Chegadas Turistas Internacionais do Brasil](https://dados.gov.br/dados/conjuntos-dados/estimativas-de-chegadas-de-turistas-internacionais-ao-brasil) disponível no [GOV.BR](https://www.gov.br/pt-br) e tecnologias como **Mysql**, **PySpark** e **Python** para transformar os dados em camadas `Landing Zone`, `Raw/Bronze`, `Trusted/Silver` e `Refined/Gold`, com foco no consumo via [**Streamlit**](https://streamlit.io/).

## Sobre o Dataset

Reúne dados relativos às estimativas do fluxo de chegadas de turistas internacionais (incluem turistas estrangeiros e brasileiros que residem no exterior) ao Brasil, desagregadas por país de residência permanente, por mês e via de acesso (aérea, terrestre, marítima ou fluvial).

As bases de dados são formadas por registros administrativos de migração coletados nos postos de fronteira e cedidos ao Ministério do Turismo pela Polícia Federal. Os dados são tratados estatisticamente de acordo com o marco teórico das Recomendações Internacionais de Estatísticas de Turismo, editadas em 2008, pela ONU Turismo, com o intuito de garantir comparabilidade internacional. Esse processo de tratamento inicia-se com a depuração da base com a exclusão dos tipos de viajantes não incluídos nas estatísticas de turismo, seguida da verificação do enquadramento de cada registro na classificação de turista (visitante que inclui pernoite em sua viagem), da conversão da variável nacionalidade em país de residência permanente (que se constitui em variável chave das estatísticas de turismo) e, por fim, utiliza-se dos resultados das pesquisas de demanda turística internacional, realizadas pelo Ministério do Turismo. Ao final tem-se as estimativas do número de chegadas de turistas internacionais ao Brasil.

## Diagrama do Projeto

![Diagrama do Projeto](https://github.com/AndersonSarmento/chegadas_turistas_internacionais_brasil/blob/main/imagens/desenho_de_solucao.png)


## Referências

Artigo: [arquitetura medallion](https://www.databricks.com/br/glossary/medallion-architecture)