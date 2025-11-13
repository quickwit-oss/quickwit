// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Box, Typography } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import ApiUrlFooter from "../components/ApiUrlFooter";
import IndexesTable from "../components/IndexesTable";
import {
  FullBoxContainer,
  QBreadcrumbs,
  ViewUnderAppBarBox,
} from "../components/LayoutUtils";
import Loader from "../components/Loader";
import ErrorResponseDisplay from "../components/ResponseErrorDisplay";
import { Client } from "../services/client";
import { IndexMetadata, ResponseError } from "../utils/models";

function IndexesView() {
  const [loading, setLoading] = useState(false);
  const [responseError, setResponseError] = useState<ResponseError | null>(
    null,
  );
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
      return (
        <FullBoxContainer sx={{ px: 0 }}>
          <IndexesTable indexesMetadata={indexesMetadata} />
        </FullBoxContainer>
      );
    }
    return <Box>You have no index registered in your metastore.</Box>;
  };

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
      },
    );
  }, [quickwitClient]);

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <QBreadcrumbs aria-label="breadcrumb">
          <Typography color="text.primary">Indexes</Typography>
        </QBreadcrumbs>
        {renderFetchIndexesResult()}
      </FullBoxContainer>
      {ApiUrlFooter("api/v1/indexes")}
    </ViewUnderAppBarBox>
  );
}

export default IndexesView;
