-- CAMBIAR SISTEMA DECIMAL
-- PRIMERO EL SIMBOLO DEL DECIMAL, LUEGO EL SEPARADOR DE MILES
alter session set nls_date_format = 'DD-MM-YYYY';
alter session set NLS_NUMERIC_CHARACTERS = '.,';

-- MOVIMIENTOS Y RIESGOS DE UN SSEGURO
select mov.sseguro, mov.nmovimi ,mov.cmotmov 
,(select tmotmov from MOTMOVSEG  where cidioma=2 and cmotmov=mov.cmotmov) MOTIVO_MOV
,mov.cmotven
,(select tmotmov from MOTMOVSEG  where cidioma=3 and cmotmov=mov.cmotven) MOTIVO_VENC
,mov.cmotanul
,mov.fmovimi,mov.femisio,mov.fefecto,mov.cmovseg
from movseguro mov 
where sseguro in (4045390,4036870,3993996)  --(3885107,3993996,4094295,4095058,4353775,4049141,4104471,4362739,4550134,4036870)
order by mov.sseguro, mov.nmovimi;

select * from riesgos where sseguro in (4045390,4036870,3993996) --(3993996,4094295,4095058,4353775,4049141,4104471,4362739,4550134) 
order by sseguro;

select * from asegurados where sseguro in (4045390,4036870,3993996) --(3993996,4094295,4095058,4353775,4049141,4104471,4362739,4550134) 
order by sseguro, nriesgo;

select * from garanseg where sseguro in (3928059,3880894)--(4045390,4036870,3993996) --(3885107,3993996,4094295,4095058,4353775,4049141,4104471,4362739,4550134) 
order by sseguro,nriesgo,nmovimi;

select * from seguros where sseguro in (4045390,4036870,3993996) --(3885107,3993996,4094295,4095058,4353775,4049141,4104471,4362739,4550134) 
order by sseguro;
-- QUERY DE EXPUESTOS V1
select
        per.NNUMIDE RUT,Decode(per.CSEXPER, 1, 'M', 2, 'F', ' ') sexo,per.FNACIMI fec_nac,per_tom.NNUMIDE rut_contratante,
        s.ccompani,s.sseguro,s.npoliza poliza,s.ncertif certificado, s.sproduc producto,r.nriesgo,
        g.cgarant cod_cob,
        s.CACTIVI COD_PLAN,act.TTITULO PLAN_DESC,
        (select crespue from  pregunpolseg pregpolseg where pregpolseg.sseguro = s.sseguro AND pregpolseg.cpregun = 36 AND pregpolseg.nmovimi IN (SELECT MAX(t3.nmovimi) FROM pregunpolseg t3 WHERE t3.cpregun = pregpolseg.cpregun AND t3.sseguro = pregpolseg.sseguro GROUP BY t3.sseguro, t3.cpregun) and rownum = 1) CANAL_VENTA,
        (SELECT COUNT(*)  from riesgos r WHERE r.sseguro = s.sseguro and r.fanulac is null ) NRO_RIESGOS,
        Nvl(Pp1.CVALPAR, 1) TIPO_POLIZA,
        s.fefecto FECHA_EFECTO,r.fefecto FECHA_EFECTO_RIESGOS,s.fvencim fecha_vencimiento,
        (select mov.fefecto from movseguro mov where s.sseguro=mov.sseguro and nmovimi=(select max(nmovimi) from movseguro where sseguro=mov.sseguro and cmovseg in (0,2) and fefecto<=Last_day(to_date('2024-01-01','yyyy-mm-dd')))) fini_renov_anual,
        s.fcaranu ffin_renov_anual,s.fanulac FECHA_ANULACION,
        g.ICAPITAL,g.iprianu,
        s.CFORPAG FORMA_PAGO,s.csituac ESTADO,
        (select tatribu from detvalores where cvalor=61 and cidioma=3 and catribu=s.csituac) DESC_ESTADO
        ,mov.nmovimi ,mov.cmotmov 
        ,(select tmotmov from MOTMOVSEG  where cidioma=2 and cmotmov=mov.cmotmov) MOTIVO_MOV
        ,mov.cmotven
        ,(select tmotmov from MOTMOVSEG  where cidioma=3 and cmotmov=mov.cmotven) MOTIVO_VENC
        ,mov.fmovimi,mov.femisio,mov.fefecto,mov.cmovseg
from
	seguros s
	left join parproductos pp1 on s.sproduc = pp1.sproduc and pp1.CPARPRO = 'TIPO_POLIZA',
	garanseg  g, per_personas per, riesgos r, (select * from tomadores where nordtom=1) t, per_personas per_tom, activisegu act, movseguro mov
where
	s.sseguro = g.sseguro
    and (g.ffinefe>to_date('2024-12-31','yyyy-mm-dd') or g.ffinefe is null)
    and g.finiefe<=to_date('2024-12-31','yyyy-mm-dd')
	and r.sseguro = s.sseguro
	and r.sperson = per.sperson
	and s.ccompani   = 1	
    and s.creteni    = 0
	and r.nriesgo=g.nriesgo	
	and r.sseguro=t.sseguro
	and t.sperson = per_tom.sperson
    and act.cidioma  = 3
    and act.cactivi  = s.cactivi
    and act.cramo    = s.cramo
    and s.fefecto<=to_date('2024-12-31','yyyy-mm-dd')
    and (s.fvencim is null or s.fvencim>to_date('2024-01-01','yyyy-mm-dd'))
    and (s.fanulac is null or s.fanulac>to_date('2024-01-01','yyyy-mm-dd'))
    and s.sproduc in (327)
    --and (s.fefecto !=r.fefecto  or s.cactivi!=r.cactivi)
    --and r.nmovimb is not null
    and mov.sseguro=r.sseguro
    ;
    
    
    

-- QUERY DE EXPUESTOS V2
select
        per.NNUMIDE RUT,Decode(per.CSEXPER, 1, 'M', 2, 'F', ' ') sexo,per.FNACIMI fec_nac,per_tom.NNUMIDE rut_contratante,
        s.ccompani,s.sseguro,s.npoliza poliza,s.ncertif certificado, s.sproduc producto,r.nriesgo,
        --g.cgarant cod_cob,
        (select cgarant from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.finiefe=(select max(finiefe) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('2024-12-31','yyyy-mm-dd')) and rownum=1 ) cgarant,
        s.CACTIVI COD_PLAN,act.TTITULO PLAN_DESC,
        (select crespue from  pregunpolseg pregpolseg where pregpolseg.sseguro = s.sseguro AND pregpolseg.cpregun = 36 AND pregpolseg.nmovimi IN (SELECT MAX(t3.nmovimi) FROM pregunpolseg t3 WHERE t3.cpregun = pregpolseg.cpregun AND t3.sseguro = pregpolseg.sseguro GROUP BY t3.sseguro, t3.cpregun) and rownum = 1) CANAL_VENTA,
        (SELECT COUNT(*)  from riesgos r WHERE r.sseguro = s.sseguro and r.fanulac is null ) NRO_RIESGOS,
        Nvl(Pp1.CVALPAR, 1) TIPO_POLIZA,
        s.fefecto FECHA_EFECTO,r.fefecto FECHA_EFECTO_RIESGOS,s.fvencim fecha_vencimiento,
        (select mov.fefecto from movseguro mov where s.sseguro=mov.sseguro and nmovimi=(select max(nmovimi) from movseguro where sseguro=mov.sseguro and cmovseg in (0,2) and fefecto<=Last_day(to_date('2024-01-01','yyyy-mm-dd')))) fini_renov_anual,
        s.fcaranu ffin_renov_anual,s.fanulac FECHA_ANULACION_POLIZA,r.fanulac FECHA_ANULACION_ASEGURADO,
        --g.ICAPITAL,g.iprianu,
        --cgarant, ICAPITAL, iprianu
        (select ICAPITAL from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.finiefe=(select max(finiefe) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('2024-12-31','yyyy-mm-dd')) and rownum=1 ) ICAPITAL,
        (select iprianu from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.finiefe=(select max(finiefe) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('2024-12-31','yyyy-mm-dd')) and rownum=1 ) iprianu,
        --(select ffinefe from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.finiefe=(select max(finiefe) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('2024-12-31','yyyy-mm-dd')) and rownum=1 ) ffinefe,
        --(select tabla.ffinefe from (select g.ffinefe from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.finiefe=(select max(finiefe) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('2024-12-31','yyyy-mm-dd')) order by  g.ffinefe desc) tabla where rownum=1) ffinefe_2,
        (select count(*) from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.finiefe=(select max(finiefe) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('2024-12-31','yyyy-mm-dd')) ) count_regs,
        s.CFORPAG FORMA_PAGO,s.csituac ESTADO,
        (select tatribu from detvalores where cvalor=61 and cidioma=3 and catribu=s.csituac) DESC_ESTADO
        ,(select cmotmov from movseguro mov where mov.sseguro=s.sseguro and mov.nmovimi = case  when r.fanulac is null then (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324) ) else (select min(nmovimi) from movseguro movi where movi.sseguro=s.sseguro and movi.cmotmov in (242)) end)  cmotmov
        --,(select cmotmov from movseguro mov where mov.sseguro=s.sseguro and mov.nmovimi = (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in case when r.fanulac is null then set(306,324) else (242) end ) ) cmotmov
        ,(select cmotmov from movseguro mov where mov.sseguro=s.sseguro and mov.nmovimi = (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324) ) ) cmotmov
        ,(select cmotven from movseguro mov where mov.sseguro=s.sseguro and mov.nmovimi = (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324)) ) cmotven
        ,(select femisio from movseguro mov where mov.sseguro=s.sseguro and mov.nmovimi = (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324)) ) femisio
        ,(select count(*) from movseguro mov where mov.sseguro=s.sseguro and cmotmov in (306,324) and mov.fefecto = (select min(fefecto) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324)) and mov.femisio = (select min(femisio) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324))) count_regs_movs
-- order by g.ffinefe desc
from
	seguros s
	left join parproductos pp1 on s.sproduc = pp1.sproduc and pp1.CPARPRO = 'TIPO_POLIZA',
	--garanseg  g,
    per_personas per, riesgos r, (select * from tomadores where nordtom=1) t, per_personas per_tom, activisegu act
where
	--s.sseguro = g.sseguro
    --and (g.ffinefe>to_date('2024-12-31','yyyy-mm-dd') or g.ffinefe is null)
    --and g.finiefe<=to_date('2024-12-31','yyyy-mm-dd')
	--and 
    r.sseguro = s.sseguro
	and r.sperson = per.sperson
	and s.ccompani   = 1	
    and s.creteni    = 0
	--and r.nriesgo=g.nriesgo	
	and r.sseguro=t.sseguro
	and t.sperson = per_tom.sperson
    and act.cidioma  = 3
    and act.cactivi  = s.cactivi
    and act.cramo    = s.cramo
    and s.fefecto<=to_date('2024-12-31','yyyy-mm-dd')
    and (s.fvencim is null or s.fvencim>to_date('2024-01-01','yyyy-mm-dd'))
    and (s.fanulac is null or s.fanulac>to_date('2024-01-01','yyyy-mm-dd'))
    and (r.fanulac is null or r.fanulac>to_date('2024-01-01','yyyy-mm-dd'))
    and s.sproduc in (327)
    --and (s.fefecto !=r.fefecto  or s.cactivi!=r.cactivi)
    --and r.nmovimb is not null
    --and mov.sseguro=r.sseguro
    --and s.sseguro in (4045390,4036870,3993996)
    order by s.sseguro,r.nriesgo
    ;
    

    


    -- BUSCA EN LA TABLA VALORES LOS CAMPOS CON CUYA DESCRIPCION COINCIDA CON LO ESCRITO
select * from valores where lower(tvalor) like '%mot%' and (cidioma=2 or cidioma=3);
-- LUEGO BUSCA EN LA TABLA DETVALORES LA DESCRIPCION DE LOS CODIGOS PARA ESE CAMPO
select val.cvalor, val.tvalor,det.catribu,det.tatribu from valores val, detvalores det where val.cvalor=det.cvalor and val.cidioma=2 and det.cidioma=2
--and val.cvalor in (73,138,156,215,250,277,291,382,413,488,575,581,586,606,607,682,690,708,739,875,876,1012,1014,1066,800054,800069,800103,800122,8001008,8001014,8001027)
and det.catribu=10
order by val.cvalor,det.CATRIBU;

select * from MOTMOVSEG where cidioma in (2,3) order by cmotmov;
select * from desmotivoanul;

select ffinefe from (select g.ffinefe from garanseg g where 3885107 = g.sseguro and 1=g.nriesgo and g.finiefe=(select max(finiefe) from garanseg gar where gar.sseguro=3885107 and gar.nriesgo=1 and gar.finiefe<=to_date('2024-12-31','yyyy-mm-dd')) order by  g.ffinefe desc) tabla where rownum=1;

-- QUERYS JIMENA RAMOS
--242 = Sacar riesgos de asegurados 243= Alta de asegurados
--673= cambio domicilio 700= Rehabilitación
Select distinct s.csituac 
,mv.cmotmov ,(select tmotmov from MOTMOVSEG  where cidioma=2 and cmotmov=mv.cmotmov) MOTIVO_MOV
,mv.cmotven,(select tmotmov from MOTMOVSEG  where cidioma=3 and cmotmov=mv.cmotven) MOTIVO_VENC ,mv.cmotanul
from movseguro mv, seguros s where mv.sseguro=s.sseguro 
--and csituac in (0,2)  
and npoliza=578 
--and cmotmov not in (100,306,324,700,286,850,673) 
--and mv.femisio>='01-09-2023' and mv.femisio<='30-06-2024'
and s.sproduc=327
order by s.csituac, mv.cmotmov, mv.cmotven
;


select npoliza,s.ncertif,s.fefecto,a.sperson,pper.nnumide,pper.TDIGITOIDE, s.FCARANU,s.csituac,a.ffecini,a.ffecfin 
from 
seguros s, asegurados a , per_personas pper where s.sseguro=a.sseguro and sproduc=344 and csituac in (0,2) and
pper.sperson=a.sperson order by npoliza;


Select S.sseguro,s.csituac,mv.NMOVIMI 
,mv.cmotmov ,(select tmotmov from MOTMOVSEG  where cidioma=2 and cmotmov=mv.cmotmov) MOTIVO_MOV
,mv.cmotven,(select tmotmov from MOTMOVSEG  where cidioma=3 and cmotmov=mv.cmotven) MOTIVO_VENC 
,mv.fmovimi,mv.femisio,mv.fefecto,mv.cmovseg
from movseguro mv, seguros s where mv.sseguro=s.sseguro 
--and csituac in (0,2)  
and npoliza=578 
and cmotmov in (242,306,324,700) 
--and mv.femisio>='01-09-2023' and mv.femisio<='30-06-2024'
and s.sproduc=327
--order by s.csituac, mv.cmotmov, mv.cmotven
order by s.sseguro,mv.fefecto,mv.femisio
;

Select s.*
,(select tatribu from detvalores where cvalor=61 and cidioma=3 and catribu=s.csituac) DESC_ESTADO
from seguros s where csituac NOT in (0,2,4)  
and npoliza=578 
and s.sproduc=327
--order by s.csituac, mv.cmotmov, mv.cmotven
order by s.sseguro
;

select (case 300 when 300 then (300,306) else (200) end) variable_creada from dual;
select case 300 when is null then (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324) );

select list(300,309) from dual;


select * from PREGUNGARANSEG where sseguro in (4045390,4036870,3993996);

select * from garanseg;