# Cloud Run Jobs da SMS-Rio
- Administrador: Pedro Marques (pedro.marques@dados.rio)

## Básico
Para simular a execução do job na sua máquina, você precisará:
1. Buildar para construir a imagem
2. Rodar a imagem num container, adicionando as variáveis de ambiente necessárias

Sabendo que:
- `[PASTA_DO_JOB]` é o nome da pasta (e não o caminho!) em que o código se encontra. Ex.: `hello_world`. Nela temos:
   - `Dockerfile`: a descrição do container
   - `requirements.txt`: as versões de pacotes para instalação
   - `main.py`: o código a ser executado
- `[NOME_IMAGEM]` é o nome que você quer dar para a imagem construida. Ex.: `hello_world` mesmo, por simplicidade.
- `[INFISICAL_ADDRESS]` é o endereço da instância self-hosted do Infisical. Peça para o Administrador.
- `[INFISICAL_TOKEN]` é o token Infisical. Peça para o Administrador.
- `[ENVIRONMENT]` é o nome do ambiente desejado. Ex.: `prod` ou `dev`

### Building - Como fazer a construção da imagem
- Rode: `docker build -t [NOME_IMAGEM] -f jobs/[PASTA_DO_JOB]/Dockerfile .`

### Running - Como rodar a imagem
- Rode: `docker run -e INFISICAL_ADDRESS=[INFISICAL_ADDRESS] -e INFISICAL_TOKEN=[INFISICAL_TOKEN] -e ENVIRONMENT=[ENVIRONMENT] --rm [NOME_IMAGEM]`

## Criando um job
- Crie uma pasta em `./jobs` com o nome do seu job
- Adicione nela:
  - uma descrição de como construir a imagem do job no `Dockerfile`;
  - o código de execução em `main.py`; e
  - um arquivo `requirements.txt` com as dependências adicionais, se necessário.

## Adicionando dependências
- Se o pacote for exclusivo para um job, liste-o no requirements.txt ou no próprio Dockerfile
- Se o pacote for utilizado em `shared/utils`, liste-o em `shared/requirements.txt`