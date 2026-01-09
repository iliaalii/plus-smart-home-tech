package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.store.*;
import ru.yandex.practicum.exception.ProductNotFoundException;
import ru.yandex.practicum.mapper.ProductMapper;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.repository.ProductRepository;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class StoreServiceImpl implements StoreService {
    private final ProductRepository productRepository;
    private final ProductMapper mapper;

    @Override
    @Transactional(readOnly = true)
    public Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable) {
        log.info("Получение продукции категории: {}", category);
        return productRepository.findByProductCategory(category, pageable).map(mapper::toDto);
    }

    @Override
    @Transactional
    public ProductDto addProduct(ProductDto productDto) {
        log.info("Добавление продукта: {}", productDto);
        Product product = mapper.toProduct(productDto);
        return mapper.toDto(productRepository.save(product));
    }

    @Override
    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        log.info("Обновление продукта: {}", productDto);

        Product product = productRepository.findById(productDto.getProductId())
                .orElseThrow(() -> new ProductNotFoundException("Продукт не найден"));

        mapper.updateProductFromDto(productDto, product);
        return mapper.toDto(productRepository.save(product));
    }

    @Override
    @Transactional
    public Boolean removeProduct(UUID productId) {
        log.info("Деактивация продукта по id: {}", productId);
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException("Продукт не найден"));

        if (product.getProductState() == ProductState.DEACTIVATE) {
            return false;
        }

        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        return true;
    }

    @Override
    @Transactional
    public Boolean updateQuantityState(UUID productId, QuantityState quantityState) {
        log.info("Обновление состояния количества товара (id): {}, на {}", productId, quantityState);
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException("Продукт не найден"));

        product.setQuantityState(quantityState);
        productRepository.save(product);
        return true;
    }

    @Override
    @Transactional(readOnly = true)
    public ProductDto getProductById(UUID productId) {
        log.info("Получение продукта по id: {}", productId);
        return mapper.toDto(productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException("Продукт не найден")));
    }
}