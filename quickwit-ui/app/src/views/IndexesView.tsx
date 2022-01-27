// Copyright (C) 2021 Quickwit, Inc.
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

import { Box, Typography, Breadcrumbs } from '@mui/material';
import { useEffect, useMemo, useState } from 'react';
import IndexesTable from '../components/IndexesTable';
import { Client } from '../services/client';
import Loader from '../components/Loader';
import { IndexMetadata } from '../utils/models';

export type ErrorResult = {
  error: string;
}
function IndexesView() {
  const [loading, setLoading] = useState(false);
  const [, setLoadingError] = useState<ErrorResult | null>(null);
  const [indexesMetadata, setIndexesMetadata] = useState<IndexMetadata[]>();
  const quickwitClient = useMemo(() => new Client(), []);

  const renderFetchIndexesResult = () => {
    if (loading || indexesMetadata === undefined) {
      return <Loader />;
    } else if (indexesMetadata.length > 0) {
      return <IndexesTable indexesMetadata={indexesMetadata} />
    } else {
      return <Box>
          You have no index registered in your metastore.
        </Box>
    }
  }

  useEffect(() => {
    setLoading(true);
    quickwitClient.listIndexes().then(
      (indexesMetadata) => {
        setLoadingError(null);
        setLoading(false);
        setIndexesMetadata(indexesMetadata);
      },
      (error) => {
        setLoading(false);
        setLoadingError({error: error});
      }
    );
  }, [quickwitClient]);

  return (
    <Box sx={{ flexGrow: 1, p: 3, marginTop: '48px'}}>
      <Breadcrumbs aria-label="breadcrumb" sx={{ paddingBottom: 5, paddingLeft: 1 }}>
        <Typography color="text.primary">Indexes</Typography>
      </Breadcrumbs>
      { renderFetchIndexesResult() }
    </Box>
  );
}

export default IndexesView;
