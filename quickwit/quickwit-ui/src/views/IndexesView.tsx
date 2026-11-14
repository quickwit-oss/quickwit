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

import AddIcon from "@mui/icons-material/Add";
import { Box, Button, Typography } from "@mui/material";
import { useCallback, useEffect, useMemo, useState } from "react";
import ApiUrlFooter from "../components/ApiUrlFooter";
import CreateIndexDialog from "../components/CreateIndexDialog";
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
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
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

  const fetchIndexes = useCallback(() => {
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

  useEffect(() => {
    fetchIndexes();
  }, [fetchIndexes]);

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <Box
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <QBreadcrumbs aria-label="breadcrumb">
            <Typography color="text.primary">Indexes</Typography>
          </QBreadcrumbs>
          <Button
            variant="contained"
            disableElevation
            disabled={loading}
            startIcon={<AddIcon />}
            onClick={() => setCreateDialogOpen(true)}
          >
            Create index
          </Button>
        </Box>
        {renderFetchIndexesResult()}
      </FullBoxContainer>
      <CreateIndexDialog
        open={createDialogOpen}
        onClose={() => setCreateDialogOpen(false)}
        onIndexCreated={() => {
          setCreateDialogOpen(false);
          fetchIndexes();
        }}
      />
      {ApiUrlFooter("api/v1/indexes")}
    </ViewUnderAppBarBox>
  );
}

export default IndexesView;
