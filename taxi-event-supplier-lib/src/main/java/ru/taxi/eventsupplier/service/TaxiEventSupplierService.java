package ru.taxi.eventsupplier.service;


import ru.taxi.eventsupplier.model.TripFinishedEvent;

public interface TaxiEventSupplierService {


    void send(TripFinishedEvent tripFinishedEvent) throws IllegalArgumentException;
}
