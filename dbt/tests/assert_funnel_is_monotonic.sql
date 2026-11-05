-- A purchase needs an add-to-cart, and an add-to-cart needs a product view, in the same session.
select session_id, page_views, add_to_carts, purchases
from {{ ref('fct_sessions') }}
where (purchases > 0 and add_to_carts = 0) or (add_to_carts > 0 and page_views = 0)
