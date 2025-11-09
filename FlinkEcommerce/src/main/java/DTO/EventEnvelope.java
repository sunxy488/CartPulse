package DTO;

import lombok.Data;

@Data
public class EventEnvelope {
    private String eventId;
    private String eventType;      // page_view / add_to_cart / checkout
    private String timestamp;      // ISO-8601 (e.g., 2025-10-24T01:23:45.000Z or with offset)

    private String userId;
    private String sessionId;
    private String cartId;
    private String currency;

    private String productId;
    private String productName;
    private String productCategory;
    private String productBrand;
    private Double productPrice;
    private Integer productQuantity;
    private Double totalAmount;

    private String transactionId;
    private String paymentMethod;

    // fields for page_view events
    private String page;
    private String referrer;
}

