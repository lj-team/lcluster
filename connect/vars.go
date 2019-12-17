package connect

// вызывать реконнет в случае переполнения буфера
var BUFFER_FULL_KILL bool = true

// отправлять C_NOP после заданного числа последовательных асинхронных команд
var NOP_AFTER int = 50

// включить кворум для чтения
var QUORUM bool = false
