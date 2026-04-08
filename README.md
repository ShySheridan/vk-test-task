# gRPC Service on Tarantool

gRPC-сервис на Java для работы с key-value хранилищем поверх Tarantool 3.2.x.

## Что реализовано

Сервис предоставляет 5 RPC-методов:

- `Put(key, value)` — сохраняет значение для нового ключа и перезаписывает значение для существующего
- `Get(key)` — возвращает значение по ключу
- `Delete(key)` — удаляет значение по ключу
- `Range(key_since, key_to)` — отдаёт gRPC stream пар `key-value` из диапазона
- `Count()` — возвращает количество записей в хранилище

## Требования задания

- Java gRPC сервис
- В качестве хранилища используется **Tarantool 3.2.x**
- Для доступа к БД используется **tarantool-java-sdk 1.5.0**
- Данные хранятся в space `KV` со схемой:

```
{
  {name = 'key', type = 'string'},
  {name = 'value', type = 'varbinary', is_nullable = true}
}
```
## Запуск проекта

### Поднять Tarantool

Из корня проекта запустить контейнер с Tarantool:

```bash
docker compose up -d