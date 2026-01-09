package ru.yandex.practicum.dto.warehouse;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AddressDto {
    private String country;
    private String city;
    private String street;
    private String house;
    private String flat;
}
