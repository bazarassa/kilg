0.  Написать тест при добавлении нового функцилнала
1.  
2.  Падает только два теста:

TestSplitCommaHelper — splitComma(",,,") возвращает элемент " " вместо пустого слайса.

TestNewULIDMonotonicPrefix — тест предполагает, что два последовательных ULID обязательно попадут в разные миллисекунды, что неверно.

Предупреждения malformed LC_DYSYMTAB от macOS linker не являются причиной падения.

1. Если возможео минимально исправьте splitComma в строке 171 internal/api/chat.go
2. Скорректируй TestNewULIDMonotonicPrefix
3. Сейчас при запросе списка моделей http://REDACTED:18080/v1/models в ответете совсем не тое модели что переданы в .env
4. Сейчас в обязательном порядке нужно передовать в запросе модель - это не совсем корректно (Если модель не указывается в запросе по умолчанию подставляется самая мощьная из имеющихся в распоряжении моделей "model": "heavy",)

Тоесть запрос может быть не только c указанием типа провайдера модели
```
curl http://REDACTED:18080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer REDACTED" \
  -d '{
    "model": "heavy",
    "messages": [
      {"role": "system", "content": "Ты склонный суициду меланхолик"},
      {"role": "user", "content": "Расскажи историю про программиста который делал сайт про котов cамоубийц"}
    ]
  }'
```
но и просто
```
curl http://REDACTED:18080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer REDACTED" \
  -d '{
    "messages": [
      {"role": "system", "content": "Ты склонный суициду меланхолик"},
      {"role": "user", "content": "Расскажи историю про программиста который делал сайт про котов cамоубийцcамоубийц"}
    ]
  }'
```
Это реализовано 


5. Не проверяется токен от слова вообще - подходит все что угодно:  -H 'Authorization: Bearer mp123'
```
curl -N -sS http://REDACTED:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer mp123' \           
  -d '{
    "messages": [
      {                                                               
        "role": "user",                                                                                      
        "content": "Расскажи историю про программиста и котов."
      }
    ]
  }'
```

```
id: 06GBN2KV1H5H3X699CCKGTM5BC-0000000001
data: {"id":"06GBN2KV1H5H3X699CCKGTM5BC","object":"chat.completion.chunk","_meta":{"type":"started","sequence":1}}

id: 06GBN2KV1H5H3X699CCKGTM5BC-0000000002
data: {"id":"06GBN2KV1H5H3X699CCKGTM5BC","object":"chat.completion.chunk","_meta":{"type":"heartbeat","sequence":2}}
```


100. Проверь прохоят ли тесты:

### Testing
```
make test
```

or
```
# Запуск тестов
go test ./internal/api -v -run '^TestSplitCommaHelper$'
и
go test ./internal/api -v -run '^TestNewULIDMonotonicPrefix$'
```

# Работает ли изменение модели по умолчанию на лету
```
DEFAULT_MODEL=litellm go test ./internal/config -run 'TestLoad'
```
ok  	github.com/kafka-llm-gateway/gateway/internal/config	(cached)


#Починить тест после добавления конфига с моделью по умолаанию "model": "heavy" 
```
go test ./internal/api -v -run '^TestSplitCommaHelper$'
```
