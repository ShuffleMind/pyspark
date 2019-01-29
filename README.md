Instalação do Docker
Para fazer o download do Docker, é necessário criar uma conta no site:
https://store.docker.com/editions/community/docker-ce-desktop-windows

Criação do container:
O comando abaixo, faz o download da imagem jupyter/pyspark-notebook do Docker hub.  E faz build do container com o nome Spark na porta 8888

docker run -it --rm -p 8888:8888 –-name spark jupyter/pyspark-notebook

Acessando o jupyter:
Jupyter notebook, é um ambiente de desenvolvimento interativo, para linguagens de programação funcional.
127.0.0.0:8888/?token=”token gerado no build do container”

Interagindo com o container
Como o Docker gera um container de desenvolvimento, logo, um container é ambiente apartado da nossa infraestrutura local. 
Para usarmos arquivos de dados dentro do Spark, primeiro, devemos efetuar a cópia para o ambiente.
Comando: docker cp C:\cnpj.csv spark:/home/jovyan
Onde, docker cp(comando de cópia) C:\cnpj.csv(arquivo local) spark:/(destino do arquivo).	
Para acessar o container, executar o comando: Docker exec -it spark /bin/bash
