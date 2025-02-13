import numpy as np
import astropy.units as u


__all__ = [
    "calc_means_covariance",
    "kbmod2pix",
    "pix2sky",
    "jac_deproject_rad",
    "calc_wcs_jacobian",
    "calc_skypos_uncerts"
]


#    x  y vx vy
# x 
# y 
# vx
# vy
def calc_means_covariance(likelihood, x, y, vx, vy):
    lexp = np.exp(likelihood)
    lexp_sum = lexp.sum()

    cov = np.nan * np.empty((4, 4))
    
    x_hat  = (x * lexp).sum() / lexp_sum
    y_hat  = (y  * lexp).sum() / lexp_sum
    vx_hat = (vx * lexp).sum() / lexp_sum
    vy_hat = (vy * lexp).sum() / lexp_sum

    # diagonals
    xx_hat   = (x**2  * lexp).sum() / lexp_sum - x_hat**2
    yy_hat   = (y**2  * lexp).sum() / lexp_sum - y_hat**2
    vxvx_hat = (vx**2 * lexp).sum() / lexp_sum - vx_hat**2
    vyvy_hat = (vy**2 * lexp).sum() / lexp_sum - vy_hat**2

    # Mixed elements
    xy_hat  = (x*y  * lexp).sum() / lexp_sum - x_hat*y_hat
    xvx_hat = (x*vx * lexp).sum() / lexp_sum - x_hat*vx_hat
    xvy_hat = (x*vy * lexp).sum() / lexp_sum - x_hat*vy_hat

    yvx_hat = (y*vx * lexp).sum() / lexp_sum - y_hat*vx_hat
    yvy_hat = (y*vy * lexp).sum() / lexp_sum - y_hat*vy_hat

    vxvy_hat = (vx*vy * lexp).sum() / lexp_sum - vx_hat*vy_hat

    cov = np.array([
        [ xx_hat,  xy_hat,  xvx_hat,  xvy_hat  ],
        [ xy_hat,  yy_hat,  yvx_hat,  yvy_hat  ],
        [ xvx_hat, yvx_hat, vxvx_hat, vxvy_hat ],
        [ xvy_hat, yvy_hat, vxvy_hat, vyvy_hat ]
    ])

    return (x_hat, y_hat, vx_hat, vy_hat), cov


# must be mjd because the v is implicitly MJD via search 
# config selection
def kbmod2pix(eigenv, cov, t1, t2, t0="start"):
    t0 = t1 if t0=="start" else t0
    
    dt1 = t1 - t0
    xinit = eigenv[0] + eigenv[2]*dt1
    yinit = eigenv[1] + eigenv[3]*dt1

    dt2 = t2 - t0
    xend = eigenv[0] + eigenv[2]*dt2
    yend = eigenv[1] + eigenv[3]*dt2

    jac = np.array([
        [1, 0, dt1,   0],
        [0, 1,   0, dt1],
        [1, 0, dt2,   0],
        [0, 1,   0, dt2]
    ])
    uncert = jac @ cov @ jac.T

    return (xinit, yinit), (xend, yend), uncert


def jac_deproject_rad(center_coord, u, v, projection):
    # sin(dec) = cos(c) sin(dec0) + v sin(c)/r cos(dec0)
    # tan(ra-ra0) = u sin(c)/r / (cos(dec0) cos(c) - v sin(dec0) sin(c)/r)
    #
    # d(sin(dec)) = cos(dec) ddec = s0 dc + (v ds + s dv) c0
    # dtan(ra-ra0) = sec^2(ra-ra0) dra
    #              = ( (u ds + s du) A - u s (dc c0 - (v ds + s dv) s0 ) )/A^2
    # where s = sin(c) / r
    #       c = cos(c)
    #       s0 = sin(dec0)
    #       c0 = cos(dec0)
    #       A = c c0 - v s s0

    rsq = u*u + v*v
    rsq1 = (u+1.e-4)**2 + v**2
    rsq2 = u**2 + (v+1.e-4)**2
    if projection is None or projection[0] == 'g':
        c = s = 1./np.sqrt(1.+rsq)
        s3 = s*s*s
        dcdu = dsdu = -u*s3
        dcdv = dsdv = -v*s3
    elif projection[0] == 's':
        s = 4. / (4.+rsq)
        c = 2.*s-1.
        ssq = s*s
        dcdu = -u * ssq
        dcdv = -v * ssq
        dsdu = 0.5*dcdu
        dsdv = 0.5*dcdv
    elif projection[0] == 'l':
        c = 1. - rsq/2.
        s = np.sqrt(4.-rsq) / 2.
        dcdu = -u
        dcdv = -v
        dsdu = -u/(4.*s)
        dsdv = -v/(4.*s)
    else:
        r = np.sqrt(rsq)
        if r == 0.:
            c = s = 1
            dcdu = -u
            dcdv = -v
            dsdu = dsdv = 0
        else:
            c = np.cos(r)
            s = np.sin(r)/r
            dcdu = -s*u
            dcdv = -s*v
            dsdu = (c-s)*u/rsq
            dsdv = (c-s)*v/rsq

    # u, v, projection
    # in Celestial Coordinates
    ra, dec = center_coord

    _sinra, _cosra = np.sin(ra), np.cos(ra)
    _sindec, _cosdec = np.sin(dec), np.cos(dec)
    
    _x = _cosdec * _cosra
    _y = _cosdec * _sinra
    _z = _sindec

    s0 = _sindec
    c0 = _cosdec
    sindec = c * s0 + v * s * c0
    cosdec = np.sqrt(1.-sindec*sindec)
    dddu = ( s0 * dcdu + v * dsdu * c0 ) / cosdec
    dddv = ( s0 * dcdv + (v * dsdv + s) * c0 ) / cosdec

    tandra_num = u * s
    tandra_denom = c * c0 - v * s * s0
    # Note: A^2 sec^2(dra) = denom^2 (1 + tan^2(dra) = denom^2 + num^2
    A2sec2dra = tandra_denom**2 + tandra_num**2
    drdu = ((u * dsdu + s) * tandra_denom - u * s * ( dcdu * c0 - v * dsdu * s0 ))/A2sec2dra
    drdv = (u * dsdv * tandra_denom - u * s * ( dcdv * c0 - (v * dsdv + s) * s0 ))/A2sec2dra

    drdu *= cosdec
    drdv *= cosdec
    
    return np.array([[drdu, drdv], [dddu, dddv]])


def calc_wcs_jacobian(wcs, x0, y0):
    if wcs.wcs.has_cd():
        cd = wcs.wcs.cd
    elif wcs.wcs.has_pc():
        cdelt1, cdelt2 = wcs.wcs.cdelt
        cd11 = wcs.wcs.pc[0, 0]*cdelt1
        cd12 = wcs.wcs.pc[0, 1]*cdelt1
        cd21 = wcs.wcs.pc[1, 0]*cdelt2
        cd22 = wcs.wcs.pc[1, 1]*cdelt2
        cd = np.array([[cd11, cd12], [cd21, cd22]])
    else:
        raise AttributeError("No CD or PC in WCS?")

    ctype = wcs.wcs.ctype[0]
    ctype = ctype.replace("RA", "")
    ctype = ctype.replace("---", "")
    if ctype in ('TAN', 'TPV', 'TNX', 'TAN-SIP'):
        projection = 'gnomonic'
    elif ctype in ('STG', 'STG-SIP'):
        projection = 'stereographic'
    elif ctype in ('ZEA', 'ZEA-SIP'):
        projection = 'lambert'
    elif ctype in ('ARC', 'ARC-SIP'):
        projection = 'postel'
    else:
        raise AttributeError("unsuported projection")

    pixc = np.array([x0, y0])
    p1 = pixc - wcs.wcs.crpix

    jac = np.diag([1, 1])

    if wcs.sip is not None:
        a_order = wcs.sip.a_order
        b_order = wcs.sip.b_order
        # Use the same order for both
        order = max(a_order, b_order)
        a, b = wcs.sip.a, wcs.sip.b

        # the calculation for SIP is differential
        # relative to CRVAL in the
        # https://fits.gsfc.nasa.gov/registry/sip/SIP_distortion_v1_0.pdf
        # but it's easier to make the first two elements identities instead
        # and not worry about translation later
        a[1,0] += 1
        b[0,1] += 1
        ab = np.array([a, b])

        x = p1[0]
        y = p1[1]
        # order = len(self.ab[0])-1
        xpow = x ** np.arange(order+1)
        ypow = y ** np.arange(order+1)
        p1 = np.dot(np.dot(ab, ypow), xpow)

        dxpow = np.zeros(order+1)
        dypow = np.zeros(order+1)
        dxpow[1:] = (np.arange(order)+1.) * xpow[:-1]
        dypow[1:] = (np.arange(order)+1.) * ypow[:-1]
        j1 = np.transpose([ np.dot(np.dot(ab, ypow), dxpow) ,
                            np.dot(np.dot(ab, dypow), xpow) ])
        jac = np.dot(j1, jac)

    # With no distorsion the jacobian is just the 
    # affine part of the WCS transform, evaluated at center
    # then shifted to the given new center coordinate and
    # scaled by units
    p2 = np.dot(cd, p1)
    jac = np.dot(cd, jac)

    unit_convert = [ -u.degree.to(u.radian), u.degree.to(u.radian) ]
    p2 *= unit_convert
    
    jac = jac * np.transpose( [ unit_convert ] )

    # Convert from (u,v) to (ra, dec)
    center_coord = wcs.wcs.crval * u.degree.to(u.rad)  
    j2 = jac_deproject_rad(center_coord, p2[0], p2[1], projection=projection)

    # rad/pix --> arcsec/pixel.
    jac = np.dot(j2, jac)
    jac *= u.radian.to(u.arcsec) 

    return jac



def pix2sky(p1, p2, uncertainties, wcs):      
    # The mean values are just directly convertable
    s1 = wcs.pixel_to_world(*p1)
    s2 = wcs.pixel_to_world(*p2)

    # the uncertainties need to be transformed
    # Fit an affine transform to a small neighborhood centered
    # on the tracklet. Purposfully overestimate the box size
    midx = (p1[0] + p2[0])/2.
    midy = (p1[1] + p2[1])/2.
    jac = calc_wcs_jacobian(wcs, midx, midy)

    J = np.array([
        [jac[0, 0], jac[0, 1],        0,         0],
        [jac[0, 1], jac[1, 1],        0,         0],
        [        0,         0, jac[0, 0], jac[0, 1]],
        [        0,         0, jac[0, 1], jac[1, 1]]
    ])
    uncert = J @ uncertainties @ J.T

    return s1, s2, uncert
    

def calc_skypos_uncerts(trajectories, mjd_start, mjd_end, wcs):
    eigenv, cov = calc_means_covariance(
        trajectories["likelihood"],
        trajectories["x"],
        trajectories["y"],
        trajectories["vx"],
        trajectories["vy"]
    )    
    p1, p2, uncert = kbmod2pix(eigenv, cov, mjd_start, mjd_end)
    s1, s2, uncert = pix2sky(p1, p2, uncert, wcs)
    return s1, s2, uncert
