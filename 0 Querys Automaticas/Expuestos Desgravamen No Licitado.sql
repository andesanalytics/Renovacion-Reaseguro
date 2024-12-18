select  
        per.NNUMIDE RUT,Decode(per.CSEXPER, 1, 'M', 2, 'F', ' ') sexo,per.FNACIMI fec_nac,per_tom.NNUMIDE rut_contratante,
        s.ccompani,s.sseguro,s.npoliza poliza,s.ncertif certificado, s.sproduc producto,r.nriesgo,cred.codoperacion nro_operacion,
        g.cgarant cod_cob,
        s.CACTIVI COD_PLAN,
        Nvl(Pp1.CVALPAR, 1) TIPO_POLIZA,
        s.fefecto FECHA_EFECTO,s.fvencim fecha_vencimiento,
        (select mov.fefecto from movseguro mov where s.sseguro=mov.sseguro and nmovimi=(select max(nmovimi) from movseguro where sseguro=mov.sseguro and cmovseg in (0,2) and fefecto<=Last_day(to_date('fecha_inicio','yyyy-mm-dd')))) fini_renov_anual,
        s.fcaranu ffin_renov_anual,s.fanulac FECHA_ANULACION,
        g.ICAPITAL,g.iprianu,
        s.CFORPAG FORMA_PAGO,s.csituac ESTADO,
        (select tatribu from detvalores where cvalor=61 and cidioma=3 and catribu=s.csituac) DESC_ESTADO,
        cred.tinteres tasa_cred,cred.ncuotas,cred.periodicidad period_tasa
from
	seguros s
    left join (select distinct sseguro,tinteres,ncuotas,periodicidad,codoperacion from prestcuadroseg where ncuotas is not null and nmovimi=1) cred on s.sseguro=cred.sseguro
    left join parproductos pp1 on s.sproduc = pp1.sproduc and pp1.CPARPRO = 'TIPO_POLIZA'
    ,garanseg  g
	,per_personas per
    ,riesgos r
   ,(select * from tomadores where nordtom=1) t
    ,per_personas per_tom
where
    s.sseguro = g.sseguro
    and (g.ffinefe>to_date('fecha_fin','yyyy-mm-dd') or g.ffinefe is null)
    and g.finiefe<=to_date('fecha_fin','yyyy-mm-dd')
    and r.sseguro = s.sseguro
	and r.sperson = per.sperson
    and s.ccompani   = 1
    and r.nriesgo = 1
    and s.creteni    = 0
    and r.nriesgo=g.nriesgo
    and r.sseguro=t.sseguro
    and t.sperson = per_tom.sperson
    and s.fefecto<=to_date('fecha_fin','yyyy-mm-dd')
    and (s.fvencim is null or s.fvencim>to_date('fecha_inicio','yyyy-mm-dd'))
    and (s.fanulac is null or s.fanulac>to_date('fecha_inicio','yyyy-mm-dd'))
    and s.sproduc not in (322,323,342,142,207,209,272,161,200,270,271,277,278,288,289,290,291,326,328,329,353,88,101,193)