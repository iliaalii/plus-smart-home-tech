package ru.yandex.practicum.service;

public interface EventService<T> {

    void send(T event);
}
