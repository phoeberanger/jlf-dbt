select * from {{ ref('dim_books') }}
where trim(book_title) = ''
