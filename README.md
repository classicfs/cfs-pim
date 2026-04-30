# Product Upsert API Documentation

> Source: Jira ticket [CFSWEB-733](https://classicfootball.atlassian.net/browse/CFSWEB-733) — comment by Soufyane Benkabouche, 8 December 2025.

## Endpoint

```
POST /V1/cfs/pim/product/upsert
```

This endpoint creates or updates products in Magento. It supports both simple and configurable products with a unified interface.

## Alumio Webhook

```
POST https://uat-classicfootballshirts.alumio.com/api/v1/webhook/pim-product-webhook
```

## Request Structure

### Creating a Simple Product (Required fields)

```json
{
    "products": [
        {
            "sku": "PRODUCT-SKU",
            "product_type": "simple",
            "name": "Anis Hadj Moussa shirt",
            "url_key": "product-sku-url",
            "websites": ["base", "eu"],
            "categories": ["Default/New Products"], // or use "category_ids": [1640]
            "price": 51.99,
            "stock": {
                "qty": 22,
                "source_name": "hyde_warehouse"
            }
        }
    ]
}
```

**Note:** Categories can be assigned using either:
- `categories`: Array of category paths (e.g., `["Default/New Products"]`).
- `category_ids`: Array of category IDs (e.g., `[1640]`).

### Creating a Configurable Product (Required fields)

```json
{
    "products": [
        {
            "sku": "PRODUCT-SKU",
            "product_type": "configurable",
            "name": "Anis Hadj Moussa shirt",
            "url_key": "product-sku-url",
            "websites": ["base", "eu"],
            "categories": ["Default/New Products"],
            "configurable_attributes": ["size_product"],
            "children": [
                {
                    "sku": "PRODUCT-SKU-M",
                    "name": "Anis Hadj Moussa shirt M",
                    "url_key": "product-sku-url-m",
                    "price": 62.99,
                    "stock": {
                        "qty": 10,
                        "source_name": "hyde_warehouse"
                    },
                    "custom_attributes": [
                        {
                            "attribute_code": "size_product",
                            "value": "M"
                        }
                    ]
                }
            ]
        }
    ]
}
```

#### Important Notes

- Configurable products **should not** include `price` or `stock` at the parent level.
- Child products automatically inherit `websites` and `categories` from the parent product.
- Each child must specify the configurable attribute value in `custom_attributes`.

## Optional Fields (Base attributes)

These attributes can be set at the product root level:

```json
{
    "tax_class_id": 22, // or use tax_class which accepts class name and pc codes
    "attribute_set_id": 4,
    "status": 1,
    "visibility": "Catalog, Search", // or use integer: 1-4
    "weight": 0.31,
    "description": "<p>Product description</p>",
    "short_description": "Short description",
    "images": [{
      // ... example below
    }]
}
```

**Note:** Tax class can be assigned using either:
- `tax_class_id`: The id of the tax class.
- `tax_class`: The class name or PC code.
- custom attributes (example below).

## Custom Attributes

Any attributes not in the base list must be defined under `custom_attributes`:

```json
{
    "custom_attributes": [
        {
            "attribute_code": "department",
            "value": "6746"
        },
        {
            "attribute_code": "b2c_pc_code",
            "value": "PC100400"
        }
    ]
}
```

**Note:** The `b2c_pc_code` and `b2b_pc_code` custom attributes will update the product tax class id if none of `tax_class_id` and `tax_class` is provided (`b2b` updates the tax class only in the eu store).

### Select / Multi-select Custom Attributes

Select and multi-select attributes support both option IDs and labels:

```json
{
    "attribute_code": "size_product",
    "value": "4"
}
// OR
{
    "attribute_code": "size_product",
    "value": "L"
}

// ---------multi select--------
{
    "attribute_code": "size_product",
    "value": ["XS", "M", "L"]
}
```

## Categories

Assign products to categories by the category path or id (root level):

```json
{
    "categories": ["Default/New Products"]
}
// OR
{
    "category_ids": [2]
}
```

## Websites

Assign products to websites using website codes (root level):

```json
{
    "sku": "PRODUCT-001",
    "name": "Product Name",
    "product_type": "simple",
    "price": 50.00,
    "attribute_set_id": 4,
    "status": 1,
    "visibility": "Catalog, Search",
    "weight": 0.31,
    "description": "<p>Product description</p>",
    "short_description": "Short description",
    "websites": ["base", "eu"]
}
```

## Visibility

Visibility can be specified as either an integer (1-4) or a label:

```json
{
    "visibility": 4
}
// OR
{
    "visibility": "Catalog, Search"
}
```

## Stock Management (MSI)

```json
{
    "stock": {
        "qty": 100,
        "is_in_stock": true,
        "manage_stock": true,
        "source_name": "hyde_warehouse"
    }
}
```

**Note:** Only `qty` and `source_name` are required.

## Store View Content (Localization)

Store-specific content can **only be set on product updates**, not during creation.

**Requirements:**
- Product must already exist.
- `store_view_codes` is required (array of store view codes).
- Any attributes specified will be updated for those store views.

```json
{
    "sku": "EXISTING-PRODUCT",
    "name": "Localized Name",
    "store_view_codes": ["us", "eseu"],
    "description": "<p>US and ES description</p>"
}
```

## Images

Images can be assigned using the `images` array in the root level of the product:

```json
{
    "images": [
        {
            "file": "/9/7/image-1.jpeg",
            "label": "Front View",
            "position": 0,
            "disabled": false,
            "types": ["image", "small_image", "thumbnail"],
            "media_type": "image"
        },
        {
            "file": "/9/7/image-2.jpeg",
            "types": ["gallery"],
            "media_type": "image"
        }
    ]
}
```

**Note:** Only `file` & `media_type` fields are required.

## Full Example

```json
{
    "products": [
        {
            "sku": "SHIRT-001",
            "name": "Classic Football Shirt",
            "url_key": "product-001",
            "product_type": "simple",
            "price": 50.00,
            "attribute_set_id": 4,
            "status": 1,
            "visibility": "Catalog, Search",
            "weight": 0.31,
            "description": "<p>Classic football shirt</p>",
            "short_description": "Football shirt",
            "websites": ["base", "eu"],
            "categories": ["Default/New Products", "Default/Premier League/Aston Villa"],
            "stock": {
                "qty": 100,
                "is_in_stock": true,
                "manage_stock": true,
                "source_name": "hyde_warehouse"
            },
            "custom_attributes": [
                {
                    "attribute_code": "size_product",
                    "value": ["XS", "M", "L"]
                },
                {
                    "attribute_code": "department",
                    "value": "6746"
                },
                {
                    "attribute_code": "colour",
                    "value": "Red"
                }
            ],
            "images": [
                {
                    "file": "/9/7/shirt-front.jpeg",
                    "types": ["image", "small_image", "thumbnail"],
                    "media_type": "image"
                },
                {
                    "file": "/9/7/shirt-back.jpeg",
                    "types": ["gallery"],
                    "media_type": "image"
                }
            ]
        }
    ]
}
```

## Error Logs

`Admin > Cfs > Pim Import logs`

## Log Retention Configuration

`Admin > Cfs > Base Configuration > Pim Import logs`
