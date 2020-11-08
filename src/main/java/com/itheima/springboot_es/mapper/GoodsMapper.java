package com.itheima.springboot_es.mapper;

import com.itheima.springboot_es.domain.Goods;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@Mapper
public interface GoodsMapper {


    /**
     * 查询所有
     */
    public List<Goods> findAll();

}
