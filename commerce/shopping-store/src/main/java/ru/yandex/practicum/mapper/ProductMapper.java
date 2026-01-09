package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.MappingTarget;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.model.Product;

@Mapper(componentModel = "spring")
public interface ProductMapper {
    ProductDto toDto(Product product);

    Product toProduct(ProductDto productDto);

    void updateProductFromDto(ProductDto dto, @MappingTarget Product product);
}