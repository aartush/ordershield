import {config} from './config';
export function errorMessage(value:unknown):string {
 if(typeof value==='string')return value;
 if(Array.isArray(value))return value.map(errorMessage).join('; ');
 if(value && typeof value==='object') {const o=value as Record<string,unknown>;if(o.msg)return `${Array.isArray(o.loc)?o.loc.filter(x=>x!=='body').join('.')+': ':''}${o.msg}`;if(o.detail)return errorMessage(o.detail);return JSON.stringify(o);}
 return 'Request failed';
}
export async function request<T>(path:string, options:{body?:unknown;signal?:AbortSignal;timeout?:number}={}):Promise<T>{
 const controller=new AbortController();const abort=()=>controller.abort();options.signal?.addEventListener('abort',abort);if(options.signal?.aborted)controller.abort();
 const timer=setTimeout(abort,options.timeout??20000);
 try{const response=await fetch(config.api+path,{method:options.body===undefined?'GET':'POST',headers:options.body===undefined?undefined:{'Content-Type':'application/json'},body:options.body===undefined?undefined:JSON.stringify(options.body),signal:controller.signal});const data:unknown=await response.json();if(!response.ok)throw new Error(`${response.status}: ${errorMessage(data)}`);return data as T;}
 catch(e){if(controller.signal.aborted&&!options.signal?.aborted)throw new Error('Request timed out. First Spark inference may require up to 180 seconds. Check the API terminal before retrying.');throw e;}
 finally{clearTimeout(timer);options.signal?.removeEventListener('abort',abort);}
}
export const fingerprint=(v:unknown)=>JSON.stringify(v);
export function exportJSON(value:unknown){const url=URL.createObjectURL(new Blob([JSON.stringify(value,null,2)],{type:'application/json'}));const a=document.createElement('a');a.href=url;a.download='ordershield-results.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
