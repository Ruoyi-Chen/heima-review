package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.sf.jsqlparser.util.validation.metadata.NamedObject.user;

@Slf4j
@Component
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1. 判断是否需要拦截 ThreadLocal里有无该用户
        if (UserHolder.getUser() == null) {
            // 没有 需要拦截,设置状态码
            response.setStatus(401);
            // 拦截
            return false;
        }
        // 有用户, 放行
        return true;
    }

    /**
     * prehandle by session
     * @param request
     * @param response
     * @param handler
     * @param ex
     * @throws Exception
     */
//    @Override
//    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        // 1. 获取session
//        HttpSession session = request.getSession();
//        // 获取请求头中的token
//
//        // 2. 获取session中的用户
//        User user = (User) session.getAttribute("user");
//
//        // 3. 判断用户是否存在
//        if (user == null) {
//            // 4. 不存在，拦截
//            response.setStatus(401);
//            return false;
//        }
//
//        // 5. 存在，保存用户信息到threadlocal
//        UserHolder.saveUser(BeanUtil.copyProperties(user, UserDTO.class));
//
//
//        // 6. 放行
//        return true;
//    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
