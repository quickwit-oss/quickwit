import { createContext, PropsWithChildren, useContext, useEffect, useState } from "react"
import { EMPTY_SEARCH_REQUEST, SearchRequest } from "../utils/models"

type Props = Record<string, unknown>;

type ContextProps = {
  lastSearchRequest: SearchRequest;
  updateLastSearchRequest: (searchRequest: SearchRequest) => void;
};

const defaultValues = {
  lastSearchRequest: EMPTY_SEARCH_REQUEST,
  updateLastSearchRequest: (_: SearchRequest) => undefined,
}

function parseSearchRequest(value: string | null): SearchRequest {
  if (value === null) {
    return EMPTY_SEARCH_REQUEST;
  }
  return JSON.parse(value);
}

export const LocalStorageContext = createContext<ContextProps>(defaultValues);

export const LocalStorageProvider = ({
  children,
}: PropsWithChildren<Props>) => {
  const [lastSearchRequest, setLastSearchRequest] = useState<SearchRequest>(EMPTY_SEARCH_REQUEST);

  useEffect(() => {
    if (localStorage.getItem('lastSearchRequest') !== null) {
      const lastSearchRequest = parseSearchRequest(localStorage.getItem('lastSearchRequest'));
      console.log('load storage', lastSearchRequest);
      setLastSearchRequest(lastSearchRequest);
    }
  }, []);

  useEffect(() => {
      localStorage.setItem('lastSearchRequest', JSON.stringify(lastSearchRequest));
  }, [lastSearchRequest]);

  function updateLastSearchRequest(searchRequest: SearchRequest) {
    setLastSearchRequest(searchRequest);
  }

  return (
    <LocalStorageContext.Provider
      value={{
        lastSearchRequest,
        updateLastSearchRequest,
        }}
    >
      {children}
    </LocalStorageContext.Provider>
  )
}

export const useLocalStorage = () => {
  return useContext(LocalStorageContext)
}