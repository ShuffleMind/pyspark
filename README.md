**Instalação do Docker**

Para fazer o download do Docker, é necessário criar uma conta no site:
https://store.docker.com/editions/community/docker-ce-desktop-windows

**Criação do container**

O comando abaixo, faz o download da imagem jupyter/pyspark-notebook do Docker hub.  E faz build do container com o nome Spark na porta 8888

docker run -it --rm -p 8888:8888 jupyter/pyspark-notebook

**Acessando o jupyter**

Jupyter notebook, é um ambiente de desenvolvimento interativo, para linguagens de programação funcional.
127.0.0.0:8888/?token=”token gerado no build do container”

**Link para download do dataset**

https://data.cityofnewyork.us/Transportation/2017-Yellow-Taxi-Trip-Data/biws-g3hs

**Interagindo com o container**

Como o Docker gera um container de desenvolvimento, logo, um container é ambiente apartado da nossa infraestrutura local. 
Para usarmos arquivos de dados dentro do Spark, primeiro, devemos efetuar a cópia para o ambiente.
Comando: docker cp C:\2017_Yellow_Taxi_Trip_Data.csv.csv spark:/home/jovyan
Para acessar o container, executar o comando: Docker exec -it spark /bin/bash
