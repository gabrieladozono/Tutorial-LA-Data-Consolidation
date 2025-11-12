# Tutorial: Como Rodar o Fluxo LA Data Consolidation no Langflow

Este tutorial explica o **passo a passo** de como rodar o **fluxo** dentro do **Langflow**, desde a configuração inicial até a execução completa.  
O objetivo é auxiliar usuários a entenderem como importar, configurar e executar o fluxo de maneira correta.

---

## 1. Baixando o Langflow

Antes de começar, é necessário ter o **Langflow** instalado no seu computador. O aplicativo está disponível na loja da IBM.

## 2. Abrindo um fluxo do tipo Basic Prompting

Com o Langflow aberto:
Clique em **“New Flow”** no canto superior direito.
Na janela de modelos, selecione o template chamado “Basic Prompting”.

<p align="center">
  <img src="imagens/basic_prompting.png" alt="Exemplo do template Basic Prompting" width="400">
</p>

## 3. Substituindo a LLM pelo modelo IBM Watson AI
O fluxo **“Basic Prompting”** vem com uma caixinha de modelo de linguagem padrão.
Antes de rodar o fluxo, vamos substituí-la pelo componente da IBM watsonx.ai.
- Clique sobre a caixa da LLM existente no fluxo e clique em **delete**
- No painel lateral esquerdo, vá até a aba Components.
- No campo de busca, digite “IBM”.
- Arraste o componente chamado **IBM watsonx.ai** para dentro do fluxo. É a segunda caixinha da lista

<p align="center">
  <img src="imagens/watsonx_ai.png" alt="Componente IBM watsonx.ai" width="200">
</p>

Conecte-o novamente à caixa de entrada (Input) e à caixa de prompt da LLM, da mesma forma que estava o modelo anterior.
Para conectar os componentesn é necessário ligar uma bolinha na outra.
> Iremos configurar esse componente mais para frente.

## 4. Definindo a descrição para a LLM
Depois de conectar o componente IBM watsonx.ai, é hora de configurar a descrição (prompt) que orienta o comportamento do modelo.
Essa descrição define como a LLM deve interpretar o pedido do usuário e gerar a saída no formato JSON correto.
- Clique no componente do watsonx.ai.  
- CLique na caixinha abaixo de templete para editar o prompt e apague o texto existente.  
- Copie o conteúdo do arquivo abaixo e cole dentro do campo de prompt:  

👉🏼 [Clique aqui para visualizar e copiar o arquivo `prompt_llm.txt`](LLM.txt)

## 5. Adicionando um componente customizado ao fluxo
Agora, vamos inserir um componente customizado no fluxo.
Esse componente contém um código personalizado que você pode importar ou colar diretamente dentro do Langflow.
- Na aba de componentes, clique em **"New Custom Component"**
- Clique sobre ela e clique em "<> code" para abrir o editor de código.

<p align="center">
  <img src="imagens/custom_component.png" alt="Custom Component" width="300">
</p>
- Copie o conteúdo do arquivo abaixo, cole dentro do editor e clique em salvar.

👉🏼 [Clique aqui para baixar o arquivo `custom_component.py`](langflow.py)
> **É necessário apagar todo o código padrão do componente antes de colar o código novo**

Por enquanto o fluxo está nesse formato:
<p align="center">
  <img src="imagens/fluxo1.png" alt="Fluxo por enquanto" width="500">
</p>








