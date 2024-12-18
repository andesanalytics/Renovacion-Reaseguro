select per.NNUMIDE RUT,Decode(per.CSEXPER, 1, 'M', 2, 'F', ' ') sexo,per.FNACIMI fec_nac,per_tom.NNUMIDE rut_contratante,
        s.ccompani,s.sseguro,R.NRIESGO,s.npoliza poliza,s.ncertif certificado, s.sproduc producto,
        (select cgarant from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.nmovimi=(select max(nmovimi) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('fecha_fin','yyyy-mm-dd')) ) cod_cob,
        s.CACTIVI COD_PLAN,act.TTITULO PLAN_DESC,
        (select crespue from  pregunpolseg pregpolseg where pregpolseg.sseguro = s.sseguro AND pregpolseg.cpregun = 36 AND pregpolseg.nmovimi IN (SELECT MAX(t3.nmovimi) FROM pregunpolseg t3 WHERE t3.cpregun = pregpolseg.cpregun AND t3.sseguro = pregpolseg.sseguro GROUP BY t3.sseguro, t3.cpregun) and rownum = 1) CANAL_VENTA,
        Nvl(Pp1.CVALPAR, 1) TIPO_POLIZA,
        r.fefecto FECHA_EFECTO,s.fvencim fecha_vencimiento,
        (select mov.fefecto from movseguro mov where s.sseguro=mov.sseguro and nmovimi=(select max(nmovimi) from movseguro where sseguro=mov.sseguro and cmovseg in (0,2) and fefecto<=Last_day(to_date('fecha_inicio','yyyy-mm-dd')))) fini_renov_anual,
        s.fcaranu ffin_renov_anual,
        (select icapital from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.nmovimi=(select max(nmovimi) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('fecha_fin','yyyy-mm-dd'))) ICAPITAL,
        (select iprianu from garanseg g where s.sseguro = g.sseguro and r.nriesgo=g.nriesgo and g.nmovimi=(select max(nmovimi) from garanseg gar where gar.sseguro=s.sseguro and gar.nriesgo=r.nriesgo and gar.finiefe<=to_date('fecha_fin','yyyy-mm-dd'))) iprianu,
        s.CFORPAG FORMA_PAGO_CODIGO,s.csituac ESTADO,
        (select tatribu from detvalores where cvalor=61 and cidioma=3 and catribu=s.csituac) DESC_ESTADO
        ,case when s.csituac =2 or r.fanulac is not null then (select cmotmov from movseguro mov where mov.sseguro=s.sseguro and mov.nmovimi = case  when r.fanulac is null then (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324) ) else (select min(nmovimi) from movseguro movi where movi.sseguro=s.sseguro and movi.cmotmov in (242)) end) end  MOTIVO_BAJA
        ,case when s.csituac =2 or r.fanulac is not null then (select cmotven from movseguro mov where mov.sseguro=s.sseguro and mov.nmovimi = case  when r.fanulac is null then (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324) ) else (select min(nmovimi) from movseguro movi where movi.sseguro=s.sseguro and movi.cmotmov in (242)) end) end  SUBMOTIVO_BAJA
        ,case when s.csituac =2 or r.fanulac is not null then (select femisio from movseguro mov where mov.sseguro=s.sseguro and mov.nmovimi = case  when r.fanulac is null then (select min(nmovimi) from movseguro mv where mv.sseguro=s.sseguro and mv.cmotmov in (306,324) ) else (select min(nmovimi) from movseguro movi where movi.sseguro=s.sseguro and movi.cmotmov in (242)) end) end  FECHA_ANULACION
from
	seguros s
	left join parproductos pp1 on s.sproduc = pp1.sproduc and pp1.CPARPRO = 'TIPO_POLIZA',
    per_personas per, riesgos r, (select * from tomadores where nordtom=1) t, per_personas per_tom, activisegu act
where
    r.sseguro = s.sseguro
	and r.sperson = per.sperson
	and s.ccompani   = 1	
    and s.creteni    = 0
	and r.sseguro=t.sseguro
	and t.sperson = per_tom.sperson
    and act.cidioma  = 3
    and act.cactivi  = s.cactivi
    and act.cramo    = s.cramo
    and s.csituac in (0,2)
    and r.fefecto<=to_date('fecha_fin','yyyy-mm-dd')
    and s.sproduc in (327)
    order by s.sseguro,r.nriesgo

    
