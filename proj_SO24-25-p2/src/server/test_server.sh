#!/bin/bash

echo "Iniciando testes do servidor para o exercício 1..."

# Limpa FIFOs antigos
echo "Limpando FIFOs antigos..."
rm -f /tmp/register_fifo /tmp/client_req* /tmp/client_resp*

# Inicia o servidor em background
echo "Iniciando servidor..."
./server ./jobs_dir 1 1 /tmp/register_fifo &
SERVER_PID=$!
sleep 1

# Verifica se o FIFO de registo foi criado enquanto o servidor está em execução
echo "Verificando FIFO de registo..."
if [ -p /tmp/register_fifo ]; then
    echo "PASS: FIFO de registo criado"
else
    echo "FAIL: FIFO de registo não foi criado"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

# Define o número de clientes para o teste
NUM_CLIENTS=4

# Cria FIFOs e conecta clientes ao servidor
for i in $(seq 1 $NUM_CLIENTS); do
    echo "Criando FIFOs do cliente $i..."
    mkfifo /tmp/client_req$i /tmp/client_resp$i
    if [ ! -p /tmp/client_req$i ] || [ ! -p /tmp/client_resp$i ]; then
        echo "FAIL: FIFOs do cliente $i não foram criados"
        kill $SERVER_PID 2>/dev/null
        exit 1
    fi

    echo "Conectando cliente $i ao servidor..."
    echo "/tmp/client_req$i;/tmp/client_resp$i" > /tmp/register_fifo
    sleep 0.2  # Aguarda para evitar concatenação de mensagens no FIFO
done

# Testa comandos para cada cliente
for i in $(seq 1 $NUM_CLIENTS); do
    echo "Iniciando sessão para cliente $i..."

    # Abre descritores do cliente
    exec {REQ_FD}>"/tmp/client_req$i"
    exec {RESP_FD}<"/tmp/client_resp$i"

    # Envia comando SUBSCRIBE
    echo "SUBSCRIBE key$i" >&$REQ_FD
    read RESPONSE <&$RESP_FD
    if [ "$RESPONSE" == "SUBSCRIBED" ]; then
        echo "PASS: SUBSCRIBE processado corretamente para cliente $i"
    else
        echo "FAIL: SUBSCRIBE não foi processado corretamente para cliente $i. Resposta: $RESPONSE"
    fi

    # Envia comando DISCONNECT
    echo "DISCONNECT" >&$REQ_FD
    sleep 1

    # Fecha os descritores
    exec {REQ_FD}>&-
    exec {RESP_FD}<&-

    # Verifica se os FIFOs do cliente foram removidos
    echo "Verificando desconexão do cliente $i..."
    if [ -p /tmp/client_req$i ] || [ -p /tmp/client_resp$i ]; then
        echo "FAIL: FIFOs do cliente $i ainda existem"
    else
        echo "PASS: Cliente $i desconectado corretamente"
    fi
done

# Finaliza o servidor
echo "Finalizando servidor..."
kill $SERVER_PID 2>/dev/null
sleep 1
if ps -p $SERVER_PID > /dev/null; then
    echo "FAIL: Não foi possível finalizar o servidor"
else
    echo "PASS: Servidor finalizado com sucesso"
fi
