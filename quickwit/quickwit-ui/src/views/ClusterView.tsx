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

import { Typography } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import ApiUrlFooter from "../components/ApiUrlFooter";
import { JsonEditor } from "../components/JsonEditor";
import {
  FullBoxContainer,
  QBreadcrumbs,
  ViewUnderAppBarBox,
} from "../components/LayoutUtils";
import Loader from "../components/Loader";
import ErrorResponseDisplay from "../components/ResponseErrorDisplay";
import { Client } from "../services/client";
import { Cluster, ResponseError } from "../utils/models";

function ClusterView() {
  const [loading, setLoading] = useState(false);
  const [cluster, setCluster] = useState<null | Cluster>(null);
  const [responseError, setResponseError] = useState<ResponseError | null>(
    null,
  );
  const quickwitClient = useMemo(() => new Client(), []);

  useEffect(() => {
    setLoading(true);
    quickwitClient.cluster().then(
      (cluster) => {
        setResponseError(null);
        setLoading(false);
        setCluster(cluster);
      },
      (error) => {
        setLoading(false);
        setResponseError(error);
      },
    );
  }, [quickwitClient]);

  const renderResult = () => {
    if (responseError !== null) {
      return ErrorResponseDisplay(responseError);
    }
    if (loading || cluster == null) {
      return <Loader />;
    }
    return <JsonEditor content={cluster} resizeOnMount={false} />;
  };

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <QBreadcrumbs aria-label="breadcrumb">
          <Typography color="text.primary">Cluster</Typography>
        </QBreadcrumbs>
        <FullBoxContainer sx={{ px: 0 }}>{renderResult()}</FullBoxContainer>
      </FullBoxContainer>
      {ApiUrlFooter("api/v1/cluster")}
    </ViewUnderAppBarBox>
  );
}

export default ClusterView;
