from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from collections import OrderedDict


class CustomTransactionPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 10

    def get_paginated_response(self, data):
        """
        Return a paginated style `Response` object with detailed pagination info
        """
        current_page = self.page.number
        page_size = self.get_page_size(self.request)
        total_count = self.page.paginator.count
        total_pages = self.page.paginator.num_pages

        # Calculate offset (0-based index of first item on current page)
        offset = (current_page - 1) * page_size

        return Response(OrderedDict([
            ('pagination', OrderedDict([
                ('page', current_page),
                ('page_size', page_size),
                ('total_pages', total_pages),
                ('total_count', total_count),
                ('offset', offset),
                ('next_url', self.get_next_link()),
                ('previous_url', self.get_previous_link()),
            ])),
            ('results', data)
        ]))
