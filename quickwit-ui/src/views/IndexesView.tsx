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

import { Box, Typography } from '@mui/material';
import { useEffect, useMemo, useState } from 'react';
import IndexesTable from '../components/IndexesTable';
import { Client } from '../services/client';
import Loader from '../components/Loader';
import { IndexMetadata, ResponseError } from '../utils/models';
import { ViewUnderAppBarBox, FullBoxContainer, QBreadcrumbs } from '../components/LayoutUtils';
import ApiUrlFooter from '../components/ApiUrlFooter';
import ErrorResponseDisplay from '../components/ResponseErrorDisplay';

function IndexesView() {
  const [loading, setLoading] = useState(false);
  const [responseError, setResponseError] = useState<ResponseError | null>(null);
  const [indexesMetadata, setIndexesMetadata] = useState<IndexMetadata[]>();
  const quickwitClient = useMemo(() => new Client(), []);

  const renderFetchIndexesResult = () => {
    if (responseError !== null) {
      return ErrorResponseDisplay(responseError);
    }
    if (loading || indexesMetadata === undefined) {
      return <Loader />;
    }
    if (indexesMetadata.length > 0) {
      return <FullBoxContainer sx={{ px: 0 }}>
          <IndexesTable indexesMetadata={indexesMetadata} />
        </FullBoxContainer>
    }
    return <Box>
        You have no index registered in your metastore.
      </Box>
  }

  useEffect(() => {
    setLoading(true);
    quickwitClient.listIndexes().then(
      (indexesMetadata) => {
        setResponseError(null);
        setLoading(false);
        setIndexesMetadata(indexesMetadata);
      },
      (error) => {
        setLoading(false);
        setResponseError(error);
      }
    );
  }, [quickwitClient]);

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <QBreadcrumbs aria-label="breadcrumb">
          <Typography color="text.primary">Indexes</Typography>
        </QBreadcrumbs>
        { renderFetchIndexesResult() }
      </FullBoxContainer>
      { ApiUrlFooter('api/v1/indexes') }
    </ViewUnderAppBarBox>
  );
}

export default IndexesView;
