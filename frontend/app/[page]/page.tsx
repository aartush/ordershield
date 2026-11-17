import {notFound} from 'next/navigation';import {ResearchPage} from '@/components/research-pages';import {nav} from '@/lib/config';
export function generateStaticParams(){return nav.filter(n=>n[2]!=='/').map(n=>({page:n[2].slice(1)}))}
export async function generateMetadata({params}:{params:Promise<{page:string}>}){const {page}=await params;return {title:nav.find(n=>n[2]==='/'+page)?.[1]||'Not found'}}
export default async function Page({params}:{params:Promise<{page:string}>}){const {page}=await params;if(!nav.some(n=>n[2]==='/'+page))notFound();return <ResearchPage page={page}/>}
