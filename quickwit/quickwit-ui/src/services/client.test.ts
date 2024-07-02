// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

import { SearchRequest } from '../utils/models';
import { Client } from './client';

describe('Client unit test', () => {
    it('Should construct correct search URL', async () => {
        // Mocking the fetch function to simulate network requests
        const mockFetch = jest.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve({}) }));
        (global as any).fetch = mockFetch; // eslint-disable-line @typescript-eslint/no-explicit-any

        const searchRequest: SearchRequest = {
            indexId: 'my-new-fresh-index-id',
            query: 'severity_error:ERROR',
            startTimestamp: 100,
            endTimestamp: 200,
            maxHits: 20,
            sortByField: {
              field_name: 'timestamp',
              order: 'Desc',
            },
            aggregation: false,
            aggregationConfig: {
              metric: null,
              term: null,
              histogram: null,
            },
        };

        const client = new Client();
        expect(client.buildSearchBody(searchRequest, null)).toBe('{"query":"severity_error:ERROR","max_hits":20,"start_timestamp":100,"end_timestamp":200,"sort_by_field":"+timestamp"}');

        await client.search(searchRequest, null);
        const expectedUrl = `${client.apiRoot()}my-new-fresh-index-id/search`;
        expect(mockFetch).toHaveBeenCalledTimes(1);
        expect(mockFetch).toHaveBeenCalledWith(expectedUrl, expect.any(Object));
    });
});
