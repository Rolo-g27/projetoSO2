#!/bin/bash

echo "Iniciando testes do servidor para o exercício 2..."

# Limpa FIFOs antigos
echo "Limpando FIFOs antigos..."
rm -f /tmp/register_fifo /tmp/client_req* /tmp/client_resp*

# Inicia o servidor em background
echo "Iniciando servidor..."
./server ./jobs_dir 1 1 /tmp/register_fifo &
SERVER_PID=$!
sleep 1

# Verifica se o FIFO de registo foi criado
echo "Verificando FIFO de registo..."
if [ -p /tmp/register_fifo ]; then
    echo "PASS: FIFO de registo criado"
else
    echo "FAIL: FIFO de registo não foi criado"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

# Define número de clientes
NUM_CLIENTS=4

# Cria FIFOs para os clientes
for i in $(seq 1 $NUM_CLIENTS); do
    if [ -p "/tmp/client_req$i" ] || [ -p "/tmp/client_resp$i" ]; then
        echo "FIFOs do cliente $i já existem. Removendo..."
        rm -f "/tmp/client_req$i" "/tmp/client_resp$i"
    fi
    echo "Criando FIFOs do cliente $i..."
    mkfifo "/tmp/client_req$i" "/tmp/client_resp$i"

    echo "Conectando cliente $i ao servidor..."
    echo "/tmp/client_req$i;/tmp/client_resp$i" > /tmp/register_fifo
    sleep 0.2
done

# Testa o cliente
for i in $(seq 1 $NUM_CLIENTS); do
    echo "Iniciando sessão para cliente $i..."

    exec {REQ_FD}>"/tmp/client_req$i"
    exec {RESP_FD}<"/tmp/client_resp$i"

    # SUBSCRIBE
    echo "SUBSCRIBE key1" >&$REQ_FD
    read RESPONSE <&$RESP_FD
    if [ "$RESPONSE" == "SUBSCRIBED" ]; then
        echo "PASS: Cliente $i inscrito corretamente na chave key1"
    else
        echo "FAIL: Cliente $i não foi inscrito corretamente. Resposta: $RESPONSE"
    fi

    # PUBLISH
    echo "PUBLISH key1 Mensagem de teste" >&$REQ_FD
    read RESPONSE <&$RESP_FD
    if [ "$RESPONSE" == "MESSAGE PUBLISHED" ]; then
        echo "PASS: Cliente $i publicou mensagem na chave key1"
    else
        echo "FAIL: Cliente $i não conseguiu publicar mensagem. Resposta: $RESPONSE"
    fi

    # UNSUBSCRIBE
    echo "UNSUBSCRIBE key1" >&$REQ_FD
    read RESPONSE <&$RESP_FD
    if [ "$RESPONSE" == "UNSUBSCRIBED" ]; then
        echo "PASS: Cliente $i cancelou inscrição corretamente"
    else
        echo "FAIL: Cliente $i não cancelou inscrição. Resposta: $RESPONSE"
    fi

    # DISCONNECT
    echo "DISCONNECT" >&$REQ_FD
    sleep 1

    exec {REQ_FD}>&-
    exec {RESP_FD}<&-

    echo "Verificando desconexão do cliente $i..."
    if [ -p "/tmp/client_req$i" ] || [ -p "/tmp/client_resp$i" ]; then
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
