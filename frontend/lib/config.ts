export const config = {brand:'OrderShield',api:process.env.NEXT_PUBLIC_API_BASE_URL || 'http://127.0.0.1:8000',inferenceTimeout:180000,motion:{micro:.16,panel:.25,entrance:.4}};
export const nav = [ ['Explore','Overview','/'],['Explore','Applications','/applications'],['Explore','Dataset','/dataset'],['Understand','Workflow','/workflow'],['Understand','How It Works','/how-it-works'],['Experiment','Samples','/samples'],['Experiment','Playground','/playground'],['Experiment','Performance','/performance'],['Observe','Live Stream','/live'],['Reference','Documentation','/documentation'] ];
export const defaultOrder = {customer_state:'SP',purchase_date:'2018-06-15',purchase_hour:12,payment_total:150,max_installments:1,payment_methods:['credit_card']};
export const categories = ['detected_risk','missed_risk','false_alert','correctly_below_threshold'];
export const label = (s:string) => s.replaceAll('_',' ').replace(/^./, c=>c.toUpperCase());
export const speakingCues:Record<string,string>={
'/':'Historical orders become review scores. The target is canceled/unavailable, not verified RTO.',
'/applications':'A human uses the queue. Review benefits and savings still require measurement.',
'/dataset':'Aggregate payments before joining. Keep one row per order and explain rare positives.',
'/workflow':'Kafka transports; Spark scores micro-batches. Incoming events do not retrain the model.',
'/how-it-works':'Separate the stored score, threshold decision and revealed historical outcome.',
'/samples':'Inspect both errors and detections. Curated categories do not estimate overall quality.',
'/playground':'Submit real inference. Scores are uncalibrated; input changes are not causal evidence.',
'/performance':'Validation selected the model and threshold. Examine missed risk and false alerts.',
'/live':'Confirm fresh heartbeat separately from saved counts and producer completion.',
'/documentation':'Describe the payment timing assumption, inspected baseline test data and recovery limits.'};
