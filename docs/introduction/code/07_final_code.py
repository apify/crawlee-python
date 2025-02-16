import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Let's limit our crawls to make our tests shorter and safer.
        max_requests_per_crawl=50,
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # We're not processing detail pages yet, so we just pass.
        if context.request.label == 'DETAIL':
            # Split the URL and get the last part to extract the manufacturer.
            url_part = context.request.url.split('/').pop()
            manufacturer = url_part.split('-')[0]

            # Extract the title using the combined selector.
            title = await context.page.locator('.product-meta h1').text_content()

            # Extract the SKU using its selector.
            sku = await context.page.locator(
                'span.product-meta__sku-number'
            ).text_content()

            # Locate the price element that contains the '$' sign and filter out
            # the visually hidden elements.
            price_element = context.page.locator('span.price', has_text='$').first
            current_price_string = await price_element.text_content() or ''
            raw_price = current_price_string.split('$')[1]
            price = float(raw_price.replace(',', ''))

            # Locate the element that contains the text 'In stock' and filter out
            # other elements.
            in_stock_element = context.page.locator(
                selector='span.product-form__inventory',
                has_text='In stock',
            ).first
            in_stock = await in_stock_element.count() > 0

            # Put it all together in a dictionary.
            data = {
                'manufacturer': manufacturer,
                'title': title,
                'sku': sku,
                'price': price,
                'in_stock': in_stock,
            }

            # Push the data to the dataset.
            await context.push_data(data)

        # We are now on a category page. We can use this to paginate through and
        # enqueue all products, as well as any subsequent pages we find.
        elif context.request.label == 'CATEGORY':
            # Wait for the product items to render.
            await context.page.wait_for_selector('.product-item > a')

            # Enqueue links found within elements matching the provided selector.
            # These links will be added to the crawling queue with the label DETAIL.
            await context.enqueue_links(
                selector='.product-item > a',
                label='DETAIL',
            )

            # Find the "Next" button to paginate through the category pages.
            next_button = await context.page.query_selector('a.pagination__next')

            # If a "Next" button is found, enqueue the next page of results.
            if next_button:
                await context.enqueue_links(
                    selector='a.pagination__next',
                    label='CATEGORY',
                )

        # This indicates we're on the start page with no specific label.
        # On the start page, we want to enqueue all the category pages.
        else:
            # Wait for the collection cards to render.
            await context.page.wait_for_selector('.collection-block-item')

            # Enqueue links found within elements matching the provided selector.
            # These links will be added to the crawling queue with the label CATEGORY.
            await context.enqueue_links(
                selector='.collection-block-item',
                label='CATEGORY',
            )

    await crawler.run(['https://warehouse-theme-metal.myshopify.com/collections'])


if __name__ == '__main__':
    asyncio.run(main())
