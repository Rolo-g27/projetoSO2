#!/bin/bash

echo "Iniciando testes do servidor..."

# Limpa FIFOs antigos
echo "Limpando FIFOs antigos..."
rm -f /tmp/register_fifo /tmp/client_req /tmp/client_resp

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

# Cria FIFOs do cliente
echo "Criando FIFOs do cliente..."
mkfifo /tmp/client_req /tmp/client_resp
if [ ! -p /tmp/client_req ] || [ ! -p /tmp/client_resp ]; then
    echo "FAIL: FIFOs do cliente não foram criados"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

# Envia informações do cliente para o FIFO de registo
echo "Conectando cliente ao servidor..."
echo "/tmp/client_req;/tmp/client_resp" > /tmp/register_fifo
sleep 1

# Verifica se o servidor recebeu a conexão
echo "Verificando conexão do cliente..."
if ! ps -p $SERVER_PID > /dev/null; then
    echo "FAIL: Servidor encerrou inesperadamente"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

# Envia comando SUBSCRIBE
echo "Enviando comando SUBSCRIBE..."
{
    echo "SUBSCRIBE key"
    sleep 1
} > /tmp/client_req &

# Lê a resposta do servidor
RESPONSE=$(head -n 1 /tmp/client_resp)
if [ "$RESPONSE" == "SUBSCRIBED" ]; then
    echo "PASS: SUBSCRIBE processado corretamente"
else
    echo "FAIL: SUBSCRIBE não foi processado corretamente. Resposta: $RESPONSE"
fi

# Envia comando DISCONNECT
echo "Enviando comando DISCONNECT..."
echo "DISCONNECT" > /tmp/client_req
sleep 1


# Verifica se os FIFOs do cliente foram removidos
echo "Verificando desconexão do cliente..."
if [ -p /tmp/client_req ] || [ -p /tmp/client_resp ]; then
    echo "FAIL: FIFOs do cliente não foram removidos após desconexão"
else
    echo "PASS: Cliente desconectado corretamente"
fi

# Finaliza o servidor
echo "Finalizando servidor..."
kill $SERVER_PID 2>/dev/null
sleep 1
if ps -p $SERVER_PID > /dev/null; then
    echo "FAIL: Não foi possível finalizar o servidor"
else
    echo "PASS: Servidor finalizado com sucesso"
fi
