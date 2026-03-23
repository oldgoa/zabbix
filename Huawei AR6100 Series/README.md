#Huawei AR6100 Series

##Шаблон, который работает с Huawei AR6100 Series и монтиорит такие метрики как:

CPU usage for 1 minute in %
Memory total
Memory usage
Memory usage %
Uptime

###Обнаружение:
Discovery fans
- Fan state

####Discovery interfaces
- High speed
- Incoming traffic
- In errors
- In Ucast pkts
- Out errors
- Outgoing traffic
- Out Ucast pkts
- Status

####Discovery optical modules
- Bias Current
- Rx high trashhold	
- Rx low trashhold
- Rx power
- Temperature
- Tx high trashhold
- Tx low trashhold
- Tx power
- Voltage

####Power Supply interfaces
- Power supply

###Триггеры:
- Утилизация памяти более 80%
- Утилизация процессора превысила 80%
- Вентилятор в "аномальном" статусе
- Большое количество ошибок по интерфейсу
- Интерфейс в состоянии Down
- Скорость канала изменилась
- Мощность передачи входящих данных оптического модуля выше допустимой
- Мощность передачи входящих данных оптического модуля ниже допустимой
- Мощность передачи исходящих данных оптического модуля выше допустимой
- Мощность передачи исходящих данных оптического модуля ниже допустимой
- Power supply DOWN

P.S. К шаблону прикручен Template Module ICMP Ping, поэтому чтобы он нормально импортировался в ваш заббикс необходимо,
чтобы данный шаблон у вас был. Либо удалите соответствующие строки в yml файле шаблона.
