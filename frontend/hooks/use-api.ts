import {useQuery} from '@tanstack/react-query';import {request} from '@/lib/api';
export function useAPI<T>(path:string,enabled=true){return useQuery<T>({queryKey:[path],queryFn:({signal})=>request<T>(path,{signal}),enabled,refetchInterval:path==='/health'?15000:false,retry:1,staleTime:30000});}
